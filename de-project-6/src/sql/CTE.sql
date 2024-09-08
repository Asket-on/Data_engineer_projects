with user_group_log as (
	SELECT hg.hk_group_id, COUNT(DISTINCT luga.hk_user_id) AS cnt_added_users
	FROM STV202311131__DWH.s_auth_history sah 
	JOIN STV202311131__DWH.l_user_group_activity luga ON sah.hk_l_user_group_activity = luga.hk_l_user_group_activity 
	JOIN STV202311131__DWH.h_groups hg ON luga.hk_group_id = hg.hk_group_id 
	WHERE event = 'add'
	GROUP BY hg.hk_group_id, hg.registration_dt 
	ORDER BY hg.registration_dt
	limit 10
)
,user_group_messages as (
	SELECT 
		lgd.hk_group_id, 
		count(DISTINCT lum.hk_user_id) AS cnt_users_in_group_with_messages
	FROM STV202311131__DWH.l_groups_dialogs lgd 
	JOIN STV202311131__DWH.h_dialogs hd ON lgd.hk_message_id = hd.hk_message_id
	JOIN STV202311131__DWH.l_user_message lum ON hd.hk_message_id = lum.hk_message_id
	GROUP BY lgd.hk_group_id 
)
select 
ugl.hk_group_id, 
cnt_added_users, 
cnt_users_in_group_with_messages,
ROUND(cnt_users_in_group_with_messages / cnt_added_users, 2) AS group_conversion
from user_group_log as ugl
left join user_group_messages as ugm on ugl.hk_group_id = ugm.hk_group_id
order by ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users DESC; 
