TRUNCATE TABLE analysis.dm_rfm_segments;

INSERT INTO analysis.dm_rfm_segments (user_id, recency, frequency, monetary_value)
WITH user_orders AS (
    SELECT 
        u.id AS user_id,
        MAX(CASE WHEN o.status = 4 THEN o.order_ts END) AS last_order_ts,
        COUNT(CASE WHEN o.status = 4 THEN o.order_id END) AS order_count,
        COALESCE(SUM(CASE WHEN o.status = 4 THEN o.cost END), 0) AS total_spend
    FROM analysis.users_w u
    LEFT JOIN analysis.orders_w o 
      ON u.id = o.user_id 
     AND o.order_ts >= '2022-01-01 00:00:00.000'
    GROUP BY u.id
),
rfm_scores AS (
    SELECT 
        user_id,
        NTILE(5) OVER (ORDER BY last_order_ts ASC NULLS FIRST) AS recency,
        NTILE(5) OVER (ORDER BY order_count ASC) AS frequency,
        NTILE(5) OVER (ORDER BY total_spend ASC) AS monetary_value
    FROM user_orders
)
SELECT user_id, recency, frequency, monetary_value
FROM rfm_scores;
