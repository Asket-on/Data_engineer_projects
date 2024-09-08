TRUNCATE analysis.tmp_rfm_frequency;
WITH t AS (
SELECT
	u.id,
	count(order_id) order_count
FROM
	(
	SELECT
		*
	FROM
		analysis.orders_w
	WHERE
		status = 4  AND order_ts >= '2022-01-01 00:00:00.000'
) t1
RIGHT JOIN analysis.users_w u
ON
	u.id = t1.user_id
GROUP BY
	u.id
)
INSERT
	INTO
	analysis.tmp_rfm_frequency
SELECT
	id,
	NTILE (5) OVER(
	ORDER BY order_count ASC ) AS frequency
FROM
	t
