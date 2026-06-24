-- Data Quality Checks for dm_courier_ledger DWH
-- Run this against your Postgres instance (de_db) to verify DWH integrity.

SELECT 
    '1. Null Values Check' AS check_name,
    COUNT(*) AS invalid_rows,
    CASE WHEN COUNT(*) = 0 THEN 'PASSED' ELSE 'FAILED' END AS status
FROM cdm.dm_courier_ledger
WHERE courier_id IS NULL 
   OR courier_name IS NULL
   OR settlement_year IS NULL 
   OR settlement_month IS NULL 
   OR orders_count IS NULL 
   OR orders_total_sum IS NULL 
   OR rate_avg IS NULL 
   OR order_processing_fee IS NULL
   OR courier_order_sum IS NULL
   OR courier_tips_sum IS NULL
   OR courier_reward_sum IS NULL

UNION ALL

SELECT 
    '2. Range and Sign Check (Non-Negative Metrics)' AS check_name,
    COUNT(*) AS invalid_rows,
    CASE WHEN COUNT(*) = 0 THEN 'PASSED' ELSE 'FAILED' END AS status
FROM cdm.dm_courier_ledger
WHERE orders_count < 0 
   OR orders_total_sum < 0.00 
   OR rate_avg < 0.00 
   OR order_processing_fee < 0.00 
   OR courier_order_sum < 0.00 
   OR courier_tips_sum < 0.00 
   OR courier_reward_sum < 0.00

UNION ALL

SELECT 
    '3. Reward Math Check (Reward = Order Sum + 95% of Tips)' AS check_name,
    COUNT(*) AS invalid_rows,
    CASE WHEN COUNT(*) = 0 THEN 'PASSED' ELSE 'FAILED' END AS status
FROM cdm.dm_courier_ledger
WHERE ABS(courier_reward_sum - (courier_order_sum + courier_tips_sum * 0.95)) > 0.01

UNION ALL

SELECT 
    '4. Courier Ledger Uniqueness Check' AS check_name,
    (
        SELECT COALESCE(SUM(c - 1), 0)
        FROM (
            SELECT COUNT(*) AS c
            FROM cdm.dm_courier_ledger
            GROUP BY courier_id, settlement_year, settlement_month
            HAVING COUNT(*) > 1
        ) t
    ) AS invalid_rows,
    CASE WHEN NOT EXISTS (
        SELECT 1 
        FROM cdm.dm_courier_ledger 
        GROUP BY courier_id, settlement_year, settlement_month 
        HAVING COUNT(*) > 1
    ) THEN 'PASSED' ELSE 'FAILED' END AS status

UNION ALL

SELECT 
    '5. Raw Delivery Ratings Check (1 to 5 Stars)' AS check_name,
    COUNT(*) AS invalid_rows,
    CASE WHEN COUNT(*) = 0 THEN 'PASSED' ELSE 'FAILED' END AS status
FROM dds.dm_api_delivery_details
WHERE rate < 1 OR rate > 5;
