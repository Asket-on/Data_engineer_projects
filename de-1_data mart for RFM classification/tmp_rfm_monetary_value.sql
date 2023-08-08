WITH ow AS (
SELECT
	u.id,
	sum(COST)
FROM
	(
	SELECT
		*
	FROM
		analysis.orders_w
	WHERE
		status = 4
		AND order_ts >= '2022-01-01 00:00:00.000') t
RIGHT JOIN analysis.users_w u
ON
	u.id = t.user_id
GROUP BY
	u.id
)
INSERT
	INTO
	analysis.tmp_rfm_monetary_value 
SELECT
	id ,
	NTILE (5) OVER(
ORDER BY
	sum ASC ) AS monetary_value
FROM
	ow