# AML Transaction Data-Quality Pipeline

This project uses Python and PySpark to validate and clean **1,025 synthetic bank
transactions** before they are used for AML monitoring. It detects duplicate
transactions, missing customer IDs, negative amounts, and settlement dates earlier
than transaction dates. Valid records are saved as clean transactions; rejected
records are kept separately with a reason for each rejection.

SQL checks inspect the results, and automated `pytest` tests verify that the rules
worked and no records went missing:

**1,025 input records = 942 clean transactions + 83 rejected transactions.**

## Rejection summary

| Rule | Problem | Rows found |
|------|---------|-----------:|
| 1 | Duplicate `transaction_id` | 25 |
| 2 | Missing `customer_id` | 20 |
| 3 | Negative `amount` | 18 |
| 4 | Settlement date **before** the transaction date | 20 |
| | **Rejected in total** | **83** |

The total reconciles: **1,025 = 942 + 83**.

## Run it
Needs **Java 17 or 21** (Spark runs on Java) - see `../START_HERE.md`.

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python generate_data.py            # creates data/transactions_raw.csv (same file every time)
python pipeline.py                 # runs the pipeline (first start takes ~20 s)
python -m pytest -v                # 5 automated tests of the pipeline itself
```

You will see a summary table with counts before and after each rule, then 8 SQL checks. New files appear in `output/`:
`clean_transactions.csv` and `rejected_transactions.csv` (each rejected row has a `reject_reason`).

## How the pieces fit

```
generate_data.py       builds the fake data and plants a known number of each defect
pipeline.py            the pipeline: load -> apply 4 rules in order -> split clean/rejected -> SQL checks
sql/quality_checks.sql the 8 SQL checks (named blocks; paste them into Databricks SQL too)
test_pipeline.py       pytest tests that confirm 1025 / 942 / 83 and "clean data has no defects"
notebooks/             the same pipeline as a Databricks notebook (import it into a workspace)
output/                created when you run the pipeline
```

## Key ideas to understand

- **Rules are applied in order.** A row that breaks two rules is rejected by the first. So the numbers after each rule add up exactly.
- **Duplicates with a window function.** `row_number()` numbers rows that share a `transaction_id`: 1 = keep, 2+ = reject.
  Look at `apply_rules` in `pipeline.py`.
- **Reject, don't delete.** Rejected rows are saved with a reason. In real data work you must be able to explain every removed row.
- **Reconciliation.** `raw = clean + rejected`. If that is ever false, rows went missing.
- **SQL checks.** Each "should be 0" query is a test. A non-zero result means the pipeline has a bug.
- **Spark basics.** Spark is lazy: nothing runs until an *action* like `count()` or `show()`. `cache()` keeps a result in memory so it is not recomputed.
- **Databricks mapping.** raw = "bronze" table, clean = "silver" table, rejected = quarantine table.

## Try these changes yourself

1. **Add a rule.** Reject amounts above 10,000 (call it `suspiciously_large_amount`) by adding one line to `define_rules()`. Run `pipeline.py`: the clean count drops. Run `pytest`: which tests fail, and why is that the *correct* behaviour? Update the expected numbers in `test_pipeline.py` to match.
2. **Break a rule on purpose.** Swap the order of rules 1 and 2 in `define_rules()` and re-run. Do the totals change? Why not?
3. **Add a SQL check.** Add check 9 to `sql/quality_checks.sql`: count of clean transactions per `country`.
4. **Plant a new defect.** In `generate_data.py` add 5 rows with an unknown currency (`XXX`), regenerate, and write the rule that catches them.

## Using it in Databricks
Databricks has a free edition (https://www.databricks.com/learn/free-edition).
1. *Workspace -> Import* -> choose `notebooks/aml_data_quality_databricks.py`.
2. Upload `data/transactions_raw.csv` to a volume and set `RAW_PATH` in the first code cell.
3. *Run all*. The notebook logic was checked against a local Spark, but **not** inside real Databricks - run it once before claiming it.

## Talking points
- "I planted a known number of each defect so I can assert exact results: 1,025 in, 942 clean, 83 rejected."
- "Rules run in a fixed order and every rejected row keeps its reason, so the numbers reconcile."
- "I verified the output two ways: SQL checks that should return zero, and pytest tests on the pipeline."
