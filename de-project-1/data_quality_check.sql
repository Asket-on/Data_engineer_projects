-- 1. Null Values Check
-- Expectation: 0 rows returned
SELECT COUNT(*) 
FROM analysis.dm_rfm_segments 
WHERE recency IS NULL 
   OR frequency IS NULL 
   OR monetary_value IS NULL;

-- 2. Range Values Check
-- Expectation: 0 rows returned
SELECT COUNT(*) 
FROM analysis.dm_rfm_segments 
WHERE recency < 1 OR recency > 5 
   OR frequency < 1 OR frequency > 5 
   OR monetary_value < 1 OR monetary_value > 5;

-- 3. Record Count/Completeness Check
-- Expectation: 0 (difference between total users and segments count is 0)
SELECT ABS(
   (SELECT COUNT(*) FROM analysis.dm_rfm_segments) - 
   (SELECT COUNT(*) FROM analysis.users_w)
);
