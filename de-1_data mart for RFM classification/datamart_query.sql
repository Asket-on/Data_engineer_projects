TRUNCATE analysis.dm_rfm_segments;
INSERT
	INTO
	analysis.dm_rfm_segments
SELECT
	tr.user_id,
	recency,
	frequency,
	monetary_value
FROM
	analysis.tmp_rfm_recency tr
JOIN analysis.tmp_rfm_frequency tf
USING(user_id)
JOIN analysis.tmp_rfm_monetary_value tmv
USING(user_id)
	
