DELETE FROM cdm.dm_courier_ledger;

WITH cte1 AS (
    SELECT 
        f.*,
        o.order_ts AS order_ts,
        d.rate AS rate,
        c.courier_id AS courier_id,
        c.courier_name AS courier_name,
        EXTRACT(YEAR FROM o.order_ts) AS settlement_year,
        EXTRACT(MONTH FROM o.order_ts) AS settlement_month,
        AVG(d.rate) OVER (
            PARTITION BY c.courier_id, 
            EXTRACT(YEAR FROM o.order_ts), 
            EXTRACT(MONTH FROM o.order_ts)
        ) AS rate_avg
    FROM dds.fct_api_sales f
    LEFT JOIN dds.dm_api_orders o ON f.order_id = o.order_id
    LEFT JOIN dds.dm_api_delivery_details d ON f.delivery_id = d.delivery_id
    LEFT JOIN dds.dm_api_couriers c ON d.courier_id = c.courier_id
),
cte2 AS (
    SELECT
        settlement_year,
        settlement_month,
        courier_id,
        courier_name,
        rate_avg,
        tip_sum,
        order_id,
        order_sum,
        CASE
            WHEN rate_avg < 4.0 THEN GREATEST(order_sum * 0.05, 100.0)
            WHEN rate_avg < 4.5 THEN GREATEST(order_sum * 0.07, 150.0)
            WHEN rate_avg < 4.9 THEN GREATEST(order_sum * 0.08, 175.0)
            ELSE GREATEST(order_sum * 0.10, 200.0)
        END AS courier_order_sum
    FROM cte1
)
INSERT INTO cdm.dm_courier_ledger (
    settlement_year,
    settlement_month,
    courier_id,
    courier_name,
    orders_count,
    orders_total_sum,
    order_processing_fee,
    courier_order_sum,
    courier_tips_sum,
    courier_reward_sum,
    rate_avg
)
SELECT 
    settlement_year,
    settlement_month,
    courier_id,
    courier_name,
    COUNT(*) AS orders_count,
    SUM(order_sum) AS orders_total_sum,
    SUM(order_sum) * 0.25 AS order_processing_fee,
    SUM(courier_order_sum) AS courier_order_sum,
    SUM(tip_sum) AS courier_tips_sum,
    SUM(courier_order_sum) + SUM(tip_sum) * 0.95 AS courier_reward_sum,
    rate_avg
FROM cte2
GROUP BY settlement_year, settlement_month, courier_id, courier_name, rate_avg;