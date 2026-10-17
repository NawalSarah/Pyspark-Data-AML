"""AML data-quality pipeline (PySpark).

Reads raw transactions, applies four data-quality rules IN ORDER, and splits the data into
  * clean rows      -> output/clean_transactions.csv
  * rejected rows   -> output/rejected_transactions.csv  (with a reject_reason column)
then runs SQL checks that prove the result is correct.

Run:  python pipeline.py

Big idea for interviews: this is how a QA engineer *tests data* - define rules, apply them,
count what went in / out / rejected, and reconcile (raw = clean + rejected).
"""
import csv
import os
import re
import sys
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

ROOT = Path(__file__).parent
RAW_PATH = ROOT / "data" / "transactions_raw.csv"
SQL_PATH = ROOT / "sql" / "quality_checks.sql"
OUT_DIR = ROOT / "output"


# --------------------------------------------------------------------------- Spark
def create_spark():
    # Make Spark use the same Python that is running this script (important on Windows).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("aml-data-quality")
        .config("spark.sql.shuffle.partitions", "4")   # small data -> few partitions = faster
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# --------------------------------------------------------------------------- load
def load_raw(spark, path=RAW_PATH):
    """Read the CSV and give every column the right type."""
    df = spark.read.option("header", True).csv(str(path))
    return (
        df.withColumn("amount", F.col("amount").cast("double"))
        .withColumn("transaction_date", F.to_date("transaction_date", "yyyy-MM-dd"))
        .withColumn("settlement_date", F.to_date("settlement_date", "yyyy-MM-dd"))
        # remember the original position in the file, so "keep the first duplicate" is predictable
        .withColumn("_row_id", F.monotonically_increasing_id())
    )


# --------------------------------------------------------------------------- rules
def define_rules():
    """The four rules: (name, condition that is TRUE for a BAD row).

    Rules are applied in this order. A row that breaks several rules is rejected by the first one.
    """
    return [
        ("duplicate_transaction_id", F.col("_dup_rank") > 1),
        ("missing_customer_id", F.col("customer_id").isNull() | (F.trim("customer_id") == "")),
        ("negative_amount", F.col("amount").isNull() | (F.col("amount") < 0)),
        (
            "inconsistent_date",
            F.col("transaction_date").isNull()
            | F.col("settlement_date").isNull()
            | (F.col("settlement_date") < F.col("transaction_date")),   # settled before it happened
        ),
    ]


def apply_rules(raw):
    """Returns (clean_df, rejected_df, steps) where steps is a list of dicts for the summary table."""
    # Number the rows that share a transaction_id: 1 = first copy (keep), 2+ = duplicates (reject).
    window = Window.partitionBy("transaction_id").orderBy("_row_id")
    remaining = raw.withColumn("_dup_rank", F.row_number().over(window))

    steps, rejected_parts = [], []
    for name, is_bad in define_rules():
        before = remaining.count()
        bad = remaining.filter(F.coalesce(is_bad, F.lit(False)))
        remaining = remaining.filter(~F.coalesce(is_bad, F.lit(False)))
        rejected_parts.append(bad.withColumn("reject_reason", F.lit(name)))
        steps.append({"rule": name, "before": before, "rejected": bad.count(), "after": remaining.count()})

    columns = [c for c in raw.columns if c != "_row_id"]
    clean = remaining.select(*columns)
    rejected = rejected_parts[0]
    for part in rejected_parts[1:]:
        rejected = rejected.unionByName(part)
    rejected = rejected.select(*columns, "reject_reason")
    return clean, rejected, steps


# --------------------------------------------------------------------------- SQL checks
def run_sql_checks(spark, sql_path=SQL_PATH):
    """sql/quality_checks.sql holds named queries ('-- name: ...'). Run each and print the result."""
    blocks = re.split(r"^-- name:", sql_path.read_text(), flags=re.MULTILINE)[1:]
    for block in blocks:
        title, _, query = block.partition("\n")
        print(f"\n--- SQL check: {title.strip()} ---")
        spark.sql(query.strip().rstrip(";")).show(truncate=False)


# --------------------------------------------------------------------------- output
def write_csv(df, path):
    rows = df.collect()    # fine for ~1,000 rows; avoids Spark's folder-of-part-files output
    path.parent.mkdir(exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(df.columns)
        writer.writerows([[("" if v is None else v) for v in row] for row in rows])


def print_summary(raw_count, steps, clean_count, rejected_count):
    print("\n=== DATA QUALITY SUMMARY ===")
    print(f"{'Rule':<28}{'Rows before':>12}{'Rejected':>10}{'Rows after':>12}")
    for s in steps:
        print(f"{s['rule']:<28}{s['before']:>12}{s['rejected']:>10}{s['after']:>12}")
    print(f"\nRaw rows: {raw_count}   Clean rows: {clean_count}   Rejected rows: {rejected_count}")
    ok = raw_count == clean_count + rejected_count
    print("Reconciliation (raw = clean + rejected):", "OK" if ok else "MISMATCH!")


def main():
    if not RAW_PATH.exists():
        sys.exit("Raw data not found. Run:  python generate_data.py")
    spark = create_spark()

    raw = load_raw(spark)
    raw.cache()
    clean, rejected, steps = apply_rules(raw)
    clean.cache()
    rejected.cache()

    # Register the three tables so we can query them with plain SQL.
    raw.drop("_row_id").createOrReplaceTempView("raw_transactions")
    clean.createOrReplaceTempView("clean_transactions")
    rejected.createOrReplaceTempView("rejected_transactions")

    print_summary(raw.count(), steps, clean.count(), rejected.count())
    run_sql_checks(spark)

    write_csv(clean, OUT_DIR / "clean_transactions.csv")
    write_csv(rejected, OUT_DIR / "rejected_transactions.csv")
    print(f"\nWrote files to {OUT_DIR}/")
    spark.stop()


if __name__ == "__main__":
    main()
