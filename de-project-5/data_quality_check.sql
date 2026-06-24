-- Vertica Data Quality Check Report for Data Vault 2.0 tables
-- Default schema prefix is assumed to be STV202311131. Adjust if custom prefix is used.

SELECT 
    '1. Staging Tables Loaded Check (Positive Rowcount)' AS check_name,
    (SELECT COUNT(*) FROM STV202311131__STAGING.users) AS measured_value,
    CASE WHEN (SELECT COUNT(*) FROM STV202311131__STAGING.users) > 0 THEN 'PASSED' ELSE 'FAILED' END AS status

UNION ALL

SELECT 
    '2. Users Hub Key Integrity (No Null IDs/Hashes)' AS check_name,
    COUNT(*) AS measured_value,
    CASE WHEN COUNT(*) = 0 THEN 'PASSED' ELSE 'FAILED' END AS status
FROM STV202311131__DWH.h_users
WHERE hk_user_id IS NULL OR user_id IS NULL

UNION ALL

SELECT 
    '3. Dialogue Hub Key Integrity (No Null IDs/Hashes)' AS check_name,
    COUNT(*) AS measured_value,
    CASE WHEN COUNT(*) = 0 THEN 'PASSED' ELSE 'FAILED' END AS status
FROM STV202311131__DWH.h_dialogs
WHERE hk_message_id IS NULL OR message_id IS NULL

UNION ALL

SELECT 
    '4. Links Referential Integrity (Admins Link to Hubs)' AS check_name,
    COUNT(*) AS measured_value,
    CASE WHEN COUNT(*) = 0 THEN 'PASSED' ELSE 'FAILED' END AS status
FROM STV202311131__DWH.l_admins
WHERE hk_user_id NOT IN (SELECT hk_user_id FROM STV202311131__DWH.h_users)
   OR hk_group_id NOT IN (SELECT hk_group_id FROM STV202311131__DWH.h_groups)

UNION ALL

SELECT 
    '5. Satellites Mapping Completeness (User Socdem Matches User Hub)' AS check_name,
    (SELECT COUNT(*) FROM STV202311131__DWH.h_users) - (SELECT COUNT(*) FROM STV202311131__DWH.s_user_socdem) AS measured_value,
    CASE WHEN (SELECT COUNT(*) FROM STV202311131__DWH.h_users) = (SELECT COUNT(*) FROM STV202311131__DWH.s_user_socdem) THEN 'PASSED' ELSE 'FAILED' END AS status;
