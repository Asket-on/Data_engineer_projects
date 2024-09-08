INSERT INTO STV202311131__DWH.global_metrics
WITH trans AS (SELECT *
FROM STV202311131__STAGING.transactions
WHERE status = 'done' AND account_number_from >= 0
AND TO_CHAR(transaction_dt, 'YYYY-MM-DD') = '{date}'),
cur AS (SELECT *
FROM STV202311131__STAGING.currencies
WHERE currency_code_with = 420
AND TO_CHAR(date_update, 'YYYY-MM-DD') = '{date}'
),
cte AS (SELECT 
    t.operation_id,
    t.account_number_from, 
    t.account_number_to, 
    t.currency_code, 
    t.transaction_type,
    c.date_update::date ,
    t.amount * c.currency_with_div AS amount_usa
FROM trans t
JOIN cur c
ON t.currency_code = c.currency_code
AND t.transaction_dt::date = c.date_update::date
)
SELECT 
    date_update, 
    currency_code as currency_code_from, 
    sum(amount_usa)  amount_total,
    count(operation_id) cnt_transactions,
    ROUND(count(operation_id) / count(DISTINCT(account_number_from)),3) AS avg_transactions_per_account,
    count(DISTINCT(account_number_from)) AS cnt_accounts_make_transactions
FROM cte
GROUP BY date_update, currency_code;
