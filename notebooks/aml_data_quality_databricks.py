# Databricks notebook source
# MAGIC %md
# MAGIC # AML data-quality pipeline (Databricks version)
# MAGIC Same logic as `pipeline.py`, written as a Databricks notebook.
# MAGIC
# MAGIC **How to use it**
# MAGIC 1. In Databricks: *Workspace -> Import* and choose this `.py` file (it becomes a notebook).
# MAGIC 2. Upload `data/transactions_raw.csv` (*Catalog -> Add data -> Upload to volume*, or DBFS).
# MAGIC 3. Set `RAW_PATH` in the first code cell to where you uploaded it, then **Run all**.
# MAGIC
# MAGIC `spark` and `display()` already exist in Databricks, so there is no setup code.

# COMMAND ----------

# Change this to the location of the uploaded file, for example:
#   "/Volumes/main/default/aml/transactions_raw.csv"   or   "dbfs:/FileStore/transactions_raw.csv"
RAW_PATH = "/Volumes/main/default/aml/transactions_raw.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load the raw data and fix the column types

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

raw = (
    spark.read.option("header", True).csv(RAW_PATH)
    .withColumn("amount", F.col("amount").cast("double"))
    .withColumn("transaction_date", F.to_date("transaction_date", "yyyy-MM-dd"))
    .withColumn("settlement_date", F.to_date("settlement_date", "yyyy-MM-dd"))
    .withColumn("_row_id", F.monotonically_increasing_id())   # original file order
)
print("Raw rows:", raw.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Apply the four rules in order
# MAGIC A row that breaks several rules is rejected by the first one only.

# COMMAND ----------

window = Window.partitionBy("transaction_id").orderBy("_row_id")
remaining = raw.withColumn("_dup_rank", F.row_number().over(window))

rules = [
    ("duplicate_transaction_id", F.col("_dup_rank") > 1),
    ("missing_customer_id", F.col("customer_id").isNull() | (F.trim("customer_id") == "")),
    ("negative_amount", F.col("amount").isNull() | (F.col("amount") < 0)),
    ("inconsistent_date",
     F.col("transaction_date").isNull() | F.col("settlement_date").isNull()
     | (F.col("settlement_date") < F.col("transaction_date"))),
]

rejected_parts, steps = [], []
for name, is_bad in rules:
    before = remaining.count()
    bad = remaining.filter(F.coalesce(is_bad, F.lit(False)))
    remaining = remaining.filter(~F.coalesce(is_bad, F.lit(False)))
    rejected_parts.append(bad.withColumn("reject_reason", F.lit(name)))
    steps.append((name, before, bad.count(), remaining.count()))

columns = [c for c in raw.columns if c != "_row_id"]
clean = remaining.select(*columns)
rejected = rejected_parts[0]
for part in rejected_parts[1:]:
    rejected = rejected.unionByName(part)
rejected = rejected.select(*columns, "reject_reason")

display(spark.createDataFrame(steps, ["rule", "rows_before", "rows_rejected", "rows_after"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Register the tables so we can check them with SQL

# COMMAND ----------

raw.drop("_row_id").createOrReplaceTempView("raw_transactions")
clean.createOrReplaceTempView("clean_transactions")
rejected.createOrReplaceTempView("rejected_transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. SQL checks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Record counts before and after
# MAGIC SELECT 'raw' AS table_name, COUNT(*) AS row_count FROM raw_transactions
# MAGIC UNION ALL SELECT 'clean', COUNT(*) FROM clean_transactions
# MAGIC UNION ALL SELECT 'rejected', COUNT(*) FROM rejected_transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rejected rows by reason
# MAGIC SELECT reject_reason, COUNT(*) AS rows_rejected
# MAGIC FROM rejected_transactions GROUP BY reject_reason ORDER BY rows_rejected DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reconciliation: must be 0
# MAGIC SELECT (SELECT COUNT(*) FROM raw_transactions)
# MAGIC      - (SELECT COUNT(*) FROM clean_transactions)
# MAGIC      - (SELECT COUNT(*) FROM rejected_transactions) AS difference_should_be_zero

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The clean table must have zero defects. Every number below should be 0.
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM (SELECT transaction_id FROM clean_transactions GROUP BY transaction_id HAVING COUNT(*) > 1)) AS duplicate_ids,
# MAGIC   (SELECT COUNT(*) FROM clean_transactions WHERE customer_id IS NULL OR TRIM(customer_id) = '')                       AS missing_customer_ids,
# MAGIC   (SELECT COUNT(*) FROM clean_transactions WHERE amount < 0 OR amount IS NULL)                                        AS negative_amounts,
# MAGIC   (SELECT COUNT(*) FROM clean_transactions WHERE settlement_date < transaction_date)                                  AS inconsistent_dates

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Save the results as Delta tables (optional)
# MAGIC Uncomment to keep the results in your workspace.

# COMMAND ----------

# clean.write.mode("overwrite").saveAsTable("aml_clean_transactions")
# rejected.write.mode("overwrite").saveAsTable("aml_rejected_transactions")
