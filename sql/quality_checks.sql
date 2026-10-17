-- SQL checks used to PROVE the pipeline worked. Same queries can be pasted into a Databricks SQL cell.
-- Each block starts with "-- name:" and is run by pipeline.py.
-- The tables raw_transactions, clean_transactions and rejected_transactions are created by the pipeline.

-- name: 1. Record counts (before and after)
SELECT 'raw'      AS table_name, COUNT(*) AS row_count FROM raw_transactions
UNION ALL
SELECT 'clean'    AS table_name, COUNT(*) AS row_count FROM clean_transactions
UNION ALL
SELECT 'rejected' AS table_name, COUNT(*) AS row_count FROM rejected_transactions;

-- name: 2. Rejected rows by reason
SELECT reject_reason, COUNT(*) AS rows_rejected
FROM rejected_transactions
GROUP BY reject_reason
ORDER BY rows_rejected DESC;

-- name: 3. Reconciliation - raw minus (clean + rejected) must be 0
SELECT (SELECT COUNT(*) FROM raw_transactions)
     - (SELECT COUNT(*) FROM clean_transactions)
     - (SELECT COUNT(*) FROM rejected_transactions) AS difference_should_be_zero;

-- name: 4. Clean table has no duplicate transaction IDs (should be 0)
SELECT COUNT(*) AS duplicate_ids
FROM (SELECT transaction_id FROM clean_transactions GROUP BY transaction_id HAVING COUNT(*) > 1);

-- name: 5. Clean table has no missing customer IDs (should be 0)
SELECT COUNT(*) AS missing_customer_ids
FROM clean_transactions
WHERE customer_id IS NULL OR TRIM(customer_id) = '';

-- name: 6. Clean table has no negative amounts (should be 0)
SELECT COUNT(*) AS negative_amounts
FROM clean_transactions
WHERE amount < 0 OR amount IS NULL;

-- name: 7. Clean table has no settlement date before the transaction date (should be 0)
SELECT COUNT(*) AS inconsistent_dates
FROM clean_transactions
WHERE settlement_date < transaction_date OR transaction_date IS NULL OR settlement_date IS NULL;

-- name: 8. Every rejected row exists in the raw data (should be 0 unknown IDs)
SELECT COUNT(*) AS unknown_ids
FROM rejected_transactions r
LEFT JOIN raw_transactions w ON r.transaction_id = w.transaction_id
WHERE w.transaction_id IS NULL;
