WITH t AS (
SELECT
	*,
	ROW_NUMBER () OVER(PARTITION BY u.id
ORDER BY
	order_ts_null DESC NULLS LAST) AS rownum
FROM
	(
	SELECT
		*,
		CASE
			WHEN status = 5 THEN NULL
			ELSE order_ts
		END order_ts_null
	FROM
		analysis.orders_w
	WHERE
		order_ts >= '2022-01-01 00:00:00.000'
) t1
RIGHT JOIN analysis.users_w u
ON
	u.id = t1.user_id
)
INSERT
	INTO
	analysis.tmp_rfm_recency
SELECT
	user_id,
	NTILE (5) OVER(
ORDER BY
	order_ts_null ASC NULLS FIRST ) AS recency
FROM
	(
	SELECT
		*
	FROM
		t
	WHERE
		rownum = 1) t1