import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

def get_vertica_conn_info():
    import json
    from airflow.hooks.base import BaseHook
    conn_info = BaseHook.get_connection('vertika_conn')
    
    try:
        extra_dict = conn_info.extra_dejson
    except Exception:
        extra_dict = {}
        if conn_info.extra:
            try:
                extra_dict = json.loads(conn_info.extra)
            except Exception:
                pass
                
    return {
        'host': extra_dict.get("host", conn_info.host),
        'port': int(extra_dict.get("port", conn_info.port or 5433)),
        'user': extra_dict.get("user", conn_info.login),
        'password': extra_dict.get("password", conn_info.password),
        'database': extra_dict.get("database", conn_info.schema),
        'autocommit': extra_dict.get("autocommit", True),
    }

def get_schema_prefix():
    from airflow.models import Variable
    return Variable.get('VERTICA_SCHEMA_PREFIX', default_var='stv202311131').upper()

def create_hubs():
    import vertica_python
    conn_info = get_vertica_conn_info()
    prefix = get_schema_prefix()
    
    with vertica_python.connect(**conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {prefix}__DWH;")
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {prefix}__DWH.h_users (
                hk_user_id bigint PRIMARY KEY, 
                user_id int, 
                registration_dt datetime, 
                load_dt datetime, 
                load_src varchar(20)
            )
            ORDER BY load_dt
            SEGMENTED BY hk_user_id ALL nodes
            PARTITION BY load_dt::date
            GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

            CREATE TABLE IF NOT EXISTS {prefix}__DWH.h_dialogs (
                hk_message_id bigint PRIMARY KEY, 
                message_id int, 
                message_ts datetime, 
                load_dt datetime, 
                load_src varchar(20)
            )
            ORDER BY load_dt
            SEGMENTED BY hk_message_id ALL nodes
            PARTITION BY load_dt::date
            GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

            CREATE TABLE IF NOT EXISTS {prefix}__DWH.h_groups (
                hk_group_id bigint PRIMARY KEY, 
                group_id int, 
                registration_dt datetime, 
                load_dt datetime, 
                load_src varchar(20)
            )
            ORDER BY load_dt
            SEGMENTED BY hk_group_id ALL nodes
            PARTITION BY load_dt::date
            GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
            """)

def create_links():
    import vertica_python
    conn_info = get_vertica_conn_info()
    prefix = get_schema_prefix()
    
    with vertica_python.connect(**conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {prefix}__DWH.l_user_message (
                hk_l_user_message bigint PRIMARY KEY, 
                hk_user_id bigint NOT NULL CONSTRAINT fk_l_user_message_user REFERENCES {prefix}__DWH.h_users (hk_user_id), 
                hk_message_id bigint NOT NULL CONSTRAINT fk_l_user_message_message REFERENCES {prefix}__DWH.h_dialogs (hk_message_id), 
                load_dt datetime, 
                load_src varchar(20)
            )
            ORDER BY load_dt
            SEGMENTED BY hk_user_id ALL nodes
            PARTITION BY load_dt::date
            GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

            CREATE TABLE IF NOT EXISTS {prefix}__DWH.l_admins (
                hk_l_admin_id bigint PRIMARY KEY, 
                hk_user_id bigint NOT NULL CONSTRAINT fk_l_admins_user REFERENCES {prefix}__DWH.h_users (hk_user_id), 
                hk_group_id bigint NOT NULL CONSTRAINT fk_l_admin_groups REFERENCES {prefix}__DWH.h_groups (hk_group_id), 
                load_dt datetime, 
                load_src varchar(20)
            )
            ORDER BY load_dt
            SEGMENTED BY hk_l_admin_id ALL nodes
            PARTITION BY load_dt::date
            GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

            CREATE TABLE IF NOT EXISTS {prefix}__DWH.l_groups_dialogs (
                hk_l_groups_dialogs bigint PRIMARY KEY, 
                hk_message_id bigint NOT NULL CONSTRAINT fk_l_groups_dialogs_dialogs REFERENCES {prefix}__DWH.h_dialogs (hk_message_id), 
                hk_group_id bigint NOT NULL CONSTRAINT fk_l_groups_dialogs_groups REFERENCES {prefix}__DWH.h_groups (hk_group_id), 
                load_dt datetime, 
                load_src varchar(20)
            )
            ORDER BY load_dt
            SEGMENTED BY hk_l_groups_dialogs ALL nodes
            PARTITION BY load_dt::date
            GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
                                
            CREATE TABLE IF NOT EXISTS {prefix}__DWH.l_user_group_activity (
                hk_l_user_group_activity bigint PRIMARY KEY, 
                hk_user_id bigint NOT NULL CONSTRAINT fk_l_user_group_activity_users REFERENCES {prefix}__DWH.h_users (hk_user_id), 
                hk_group_id bigint NOT NULL CONSTRAINT fk_l_user_group_activity_groups REFERENCES {prefix}__DWH.h_groups (hk_group_id), 
                load_dt datetime, 
                load_src varchar(20)
            )
            ORDER BY load_dt
            SEGMENTED BY hk_user_id ALL nodes
            PARTITION BY load_dt::date
            GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
            """)

def create_satellits():
    import vertica_python
    conn_info = get_vertica_conn_info()
    prefix = get_schema_prefix()
    
    with vertica_python.connect(**conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {prefix}__DWH.s_admins (
                hk_admin_id bigint NOT NULL CONSTRAINT fk_s_admins_l_admins REFERENCES {prefix}__DWH.l_admins (hk_l_admin_id), 
                is_admin boolean, 
                admin_from datetime, 
                load_dt datetime, 
                load_src varchar(20)
            )
            ORDER BY load_dt
            SEGMENTED BY hk_admin_id ALL nodes
            PARTITION BY load_dt::date
            GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
                                                        
            CREATE TABLE IF NOT EXISTS {prefix}__DWH.s_group_name (
                hk_group_id bigint NOT NULL CONSTRAINT fk_s_group_name_h_groups REFERENCES {prefix}__DWH.h_groups (hk_group_id),
                group_name varchar(100),
                load_dt datetime,
                load_src varchar(20)
            )
            ORDER BY load_dt
            SEGMENTED BY hk_group_id ALL nodes
            PARTITION BY load_dt::date
            GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

            CREATE TABLE IF NOT EXISTS {prefix}__DWH.s_group_private_status (
                hk_group_id bigint NOT NULL CONSTRAINT fk_s_group_private_status_h_groups REFERENCES {prefix}__DWH.h_groups (hk_group_id), 
                is_private boolean, 
                load_dt datetime, 
                load_src varchar(20)
            )
            ORDER BY load_dt
            SEGMENTED BY hk_group_id ALL nodes
            PARTITION BY load_dt::date
            GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);                        

            CREATE TABLE IF NOT EXISTS {prefix}__DWH.s_dialog_info (
                hk_message_id bigint NOT NULL CONSTRAINT fk_s_dialog_info_h_dialogs REFERENCES {prefix}__DWH.h_dialogs (hk_message_id), 
                message varchar(1000), 
                message_from integer, 
                message_to integer, 
                load_dt datetime, 
                load_src varchar(20)
            )
            ORDER BY load_dt
            SEGMENTED BY hk_message_id ALL nodes
            PARTITION BY load_dt::date
            GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
                                
            CREATE TABLE IF NOT EXISTS {prefix}__DWH.s_user_socdem (
                hk_user_id bigint NOT NULL CONSTRAINT fk_s_user_socdem_h_users REFERENCES {prefix}__DWH.h_users (hk_user_id), 
                country varchar(200), 
                age integer, 
                load_dt datetime, 
                load_src varchar(20)
            )
            ORDER BY load_dt
            SEGMENTED BY hk_user_id ALL nodes
            PARTITION BY load_dt::date
            GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

            CREATE TABLE IF NOT EXISTS {prefix}__DWH.s_user_chatinfo (
                hk_user_id bigint NOT NULL CONSTRAINT fk_s_user_chatinfo_h_users REFERENCES {prefix}__DWH.h_users (hk_user_id), 
                chat_name varchar(200), 
                load_dt datetime, 
                load_src varchar(20)
            )
            ORDER BY load_dt
            SEGMENTED BY hk_user_id ALL nodes
            PARTITION BY load_dt::date
            GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
                                                
            CREATE TABLE IF NOT EXISTS {prefix}__DWH.s_auth_history (
                hk_l_user_group_activity bigint NOT NULL CONSTRAINT fk_s_auth_history_l_uga REFERENCES {prefix}__DWH.l_user_group_activity (hk_l_user_group_activity), 
                user_id_from bigint, 
                event varchar(200), 
                event_dt datetime, 
                load_dt datetime, 
                load_src varchar(20)
            )
            ORDER BY load_dt
            SEGMENTED BY hk_l_user_group_activity ALL nodes
            PARTITION BY load_dt::date
            GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
            """)  

def fill_hubs():
    import vertica_python
    conn_info = get_vertica_conn_info()
    prefix = get_schema_prefix()
    
    with vertica_python.connect(**conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
            -- Optimized duplicate checks using NOT EXISTS for better MPP performance
            INSERT INTO {prefix}__DWH.h_users(hk_user_id, user_id, registration_dt, load_dt, load_src)
            SELECT hash(id) AS hk_user_id, id AS user_id, registration_dt, now() AS load_dt, 's3' AS load_src
            FROM {prefix}__STAGING.users u
            WHERE NOT EXISTS (
                SELECT 1 FROM {prefix}__DWH.h_users h WHERE h.hk_user_id = hash(u.id)
            );

            INSERT INTO {prefix}__DWH.h_dialogs(hk_message_id, message_id, message_ts, load_dt, load_src)
            SELECT hash(message_id) AS hk_message_id, message_id AS message_id, message_ts, now() AS load_dt, 's3' AS load_src
            FROM {prefix}__STAGING.dialogs d
            WHERE NOT EXISTS (
                SELECT 1 FROM {prefix}__DWH.h_dialogs h WHERE h.hk_message_id = hash(d.message_id)
            );

            INSERT INTO {prefix}__DWH.h_groups(hk_group_id, group_id, registration_dt, load_dt, load_src)
            SELECT hash(id) AS hk_group_id, id AS group_id, registration_dt, now() AS load_dt, 's3' AS load_src
            FROM {prefix}__STAGING.groups g
            WHERE NOT EXISTS (
                SELECT 1 FROM {prefix}__DWH.h_groups h WHERE h.hk_group_id = hash(g.id)
            );
            """)

def fill_links():
    import vertica_python
    conn_info = get_vertica_conn_info()
    prefix = get_schema_prefix()
    
    with vertica_python.connect(**conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
            -- l_admins Optimized Load
            INSERT INTO {prefix}__DWH.l_admins(hk_l_admin_id, hk_group_id, hk_user_id, load_dt, load_src)
            SELECT hash(hg.hk_group_id, hu.hk_user_id), hg.hk_group_id, hu.hk_user_id, now() AS load_dt, 's3' AS load_src
            FROM {prefix}__STAGING.groups AS g
            LEFT JOIN {prefix}__DWH.h_users AS hu ON g.admin_id = hu.user_id
            LEFT JOIN {prefix}__DWH.h_groups AS hg ON g.id = hg.group_id
            WHERE hg.hk_group_id IS NOT NULL AND hu.hk_user_id IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM {prefix}__DWH.l_admins l WHERE l.hk_l_admin_id = hash(hg.hk_group_id, hu.hk_user_id)
            );

            -- l_groups_dialogs Optimized Load
            INSERT INTO {prefix}__DWH.l_groups_dialogs(hk_l_groups_dialogs, hk_message_id, hk_group_id, load_dt, load_src)
            SELECT hash(hg.hk_group_id, hd.hk_message_id), hd.hk_message_id, hg.hk_group_id, now() AS load_dt, 's3' AS load_src
            FROM {prefix}__STAGING.dialogs AS d
            JOIN {prefix}__DWH.h_dialogs AS hd ON d.message_id = hd.message_id
            JOIN {prefix}__DWH.h_groups AS hg ON d.message_group = hg.group_id
            WHERE NOT EXISTS (
                SELECT 1 FROM {prefix}__DWH.l_groups_dialogs l WHERE l.hk_l_groups_dialogs = hash(hg.hk_group_id, hd.hk_message_id)
            );

            -- l_user_message Optimized Load
            INSERT INTO {prefix}__DWH.l_user_message (hk_l_user_message, hk_user_id, hk_message_id, load_dt, load_src)
            SELECT hash(hu.hk_user_id, hd.hk_message_id), hu.hk_user_id, hd.hk_message_id, now() AS load_dt, 's3' AS load_src
            FROM {prefix}__STAGING.dialogs AS d
            LEFT JOIN {prefix}__DWH.h_dialogs AS hd ON d.message_id = hd.message_id
            LEFT JOIN {prefix}__DWH.h_users AS hu ON d.message_from = hu.user_id
            WHERE hu.hk_user_id IS NOT NULL AND hd.hk_message_id IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM {prefix}__DWH.l_user_message l WHERE l.hk_l_user_message = hash(hu.hk_user_id, hd.hk_message_id)
            );
            
            -- l_user_group_activity Optimized Load
            INSERT INTO {prefix}__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
            SELECT DISTINCT
                    hash(hg.hk_group_id, hu.hk_user_id), hu.hk_user_id, hg.hk_group_id, now() AS load_dt, 's3' AS load_src
            FROM {prefix}__STAGING.group_log AS gl
            LEFT JOIN {prefix}__DWH.h_users hu ON gl.user_id = hu.user_id
            LEFT JOIN {prefix}__DWH.h_groups hg ON gl.group_id = hg.group_id
            WHERE hg.hk_group_id IS NOT NULL AND hu.hk_user_id IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM {prefix}__DWH.l_user_group_activity l WHERE l.hk_l_user_group_activity = hash(hg.hk_group_id, hu.hk_user_id)
            );
            """) 

def fill_satellits():
    import vertica_python
    conn_info = get_vertica_conn_info()
    prefix = get_schema_prefix()
    
    with vertica_python.connect(**conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
            -- s_admins Idempotent Load
            INSERT INTO {prefix}__DWH.s_admins(hk_admin_id, is_admin, admin_from, load_dt, load_src)
            SELECT la.hk_l_admin_id, TRUE AS is_admin, hg.registration_dt, now() AS load_dt, 's3' AS load_src
            FROM {prefix}__DWH.l_admins AS la
            LEFT JOIN {prefix}__DWH.h_groups AS hg ON la.hk_group_id = hg.hk_group_id
            WHERE NOT EXISTS (
                SELECT 1 FROM {prefix}__DWH.s_admins s WHERE s.hk_admin_id = la.hk_l_admin_id
            );
        
            -- s_group_name Idempotent Load
            INSERT INTO {prefix}__DWH.s_group_name(hk_group_id, group_name, load_dt, load_src)
            SELECT hg.hk_group_id, g.group_name, now() AS load_dt, 's3' AS load_src
            FROM {prefix}__STAGING.groups AS g
            LEFT JOIN {prefix}__DWH.h_groups AS hg ON g.id = hg.group_id
            WHERE hg.hk_group_id IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM {prefix}__DWH.s_group_name s WHERE s.hk_group_id = hg.hk_group_id
            );
        
            -- s_group_private_status Idempotent Load
            INSERT INTO {prefix}__DWH.s_group_private_status(hk_group_id, is_private, load_dt, load_src)
            SELECT hg.hk_group_id, g.is_private, now() AS load_dt, 's3' AS load_src
            FROM {prefix}__STAGING.groups AS g
            LEFT JOIN {prefix}__DWH.h_groups AS hg ON g.id = hg.group_id
            WHERE hg.hk_group_id IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM {prefix}__DWH.s_group_private_status s WHERE s.hk_group_id = hg.hk_group_id
            );                        
        
            -- s_dialog_info Idempotent Load
            INSERT INTO {prefix}__DWH.s_dialog_info(hk_message_id, message, message_from, message_to, load_dt, load_src)
            SELECT hd.hk_message_id, d.message, d.message_from, d.message_to, now() AS load_dt, 's3' AS load_src
            FROM {prefix}__STAGING.dialogs AS d
            LEFT JOIN {prefix}__DWH.h_dialogs AS hd ON d.message_id = hd.message_id
            WHERE hd.hk_message_id IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM {prefix}__DWH.s_dialog_info s WHERE s.hk_message_id = hd.hk_message_id
            );
                                
            -- s_user_socdem Idempotent Load
            INSERT INTO {prefix}__DWH.s_user_socdem(hk_user_id, country, age, load_dt, load_src)
            SELECT hu.hk_user_id, u.country, u.age, now() AS load_dt, 's3' AS load_src
            FROM {prefix}__STAGING.users AS u
            LEFT JOIN {prefix}__DWH.h_users AS hu ON u.id = hu.user_id
            WHERE hu.hk_user_id IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM {prefix}__DWH.s_user_socdem s WHERE s.hk_user_id = hu.hk_user_id
            );
        
            -- s_user_chatinfo Idempotent Load
            INSERT INTO {prefix}__DWH.s_user_chatinfo(hk_user_id, chat_name, load_dt, load_src)
            SELECT hu.hk_user_id, u.chat_name, now() AS load_dt, 's3' AS load_src
            FROM {prefix}__STAGING.users AS u
            LEFT JOIN {prefix}__DWH.h_users AS hu ON u.id = hu.user_id
            WHERE hu.hk_user_id IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM {prefix}__DWH.s_user_chatinfo s WHERE s.hk_user_id = hu.hk_user_id
            );
                                                
            -- s_auth_history Idempotent Load
            INSERT INTO {prefix}__DWH.s_auth_history(hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)
            SELECT hash(hg.hk_group_id, hu.hk_user_id), gl.user_id_from, gl.event, gl.datetime_ AS event_dt, now() AS load_dt, 's3' AS load_src
            FROM {prefix}__STAGING.group_log AS gl
            LEFT JOIN {prefix}__DWH.h_groups AS hg ON gl.group_id = hg.group_id
            LEFT JOIN {prefix}__DWH.h_users AS hu ON gl.user_id = hu.user_id
            LEFT JOIN {prefix}__DWH.l_user_group_activity AS luga ON hg.hk_group_id = luga.hk_group_id AND hu.hk_user_id = luga.hk_user_id
            WHERE luga.hk_l_user_group_activity IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM {prefix}__DWH.s_auth_history s 
                WHERE s.hk_l_user_group_activity = luga.hk_l_user_group_activity
                  AND s.event_dt = gl.datetime_
                  AND s.event = gl.event
            );
            """)  

with DAG(
    dag_id='sp6_project_dag_dds',
    schedule_interval=None,
    start_date=pendulum.parse('2022-07-13'),  
    catchup=False,                   
    tags=['sprint6', 'project', 'dds'],
) as dag:
    
    with TaskGroup("create_dds") as create_dds:
        t_create_hubs = PythonOperator(
            task_id='create_hubs',
            python_callable=create_hubs,
        )
        t_create_links = PythonOperator(
            task_id='create_links',
            python_callable=create_links,
        )
        t_create_satellits = PythonOperator(
            task_id='create_satellits',
            python_callable=create_satellits,
        )
        
        # Enforce DDL ordering dependencies to respect Data Vault FK constraints
        t_create_hubs >> t_create_links >> t_create_satellits

    with TaskGroup("fill_dds") as fill_dds:
        t_fill_hubs = PythonOperator(
            task_id='fill_hubs',
            python_callable=fill_hubs,
        )
        t_fill_links = PythonOperator(
            task_id='fill_links',
            python_callable=fill_links,
        )
        t_fill_satellits = PythonOperator(
            task_id='fill_satellits',
            python_callable=fill_satellits,
        )
        
        # Data Vault load sequence dependencies
        t_fill_hubs >> t_fill_links >> t_fill_satellits

    create_dds >> fill_dds
