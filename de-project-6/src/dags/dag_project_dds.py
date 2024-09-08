from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.utils.task_group import TaskGroup
import pendulum
import vertica_python
import json

conn_info = BaseHook.get_connection('vertika_conn')

extra_dict = json.loads(conn_info.extra)

vertica_conn_info = {
    'host': extra_dict.get("host"),
    'port': extra_dict.get("port"),
    'user': extra_dict.get("user"),
    'password': extra_dict.get("password"),
    'database': extra_dict.get("database"),
    'autocommit': extra_dict.get("autocommit"),
}

def create_hubs():
    with vertica_python.connect(**vertica_conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute("""
    CREATE TABLE STV202311131__DWH.h_users
    (
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

    CREATE TABLE STV202311131__DWH.h_dialogs
    (
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

    CREATE TABLE STV202311131__DWH.h_groups
    (
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
    with vertica_python.connect(**vertica_conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute("""
    CREATE TABLE STV202311131__DWH.l_user_message
    (
    hk_l_user_message bigint PRIMARY KEY, 
    hk_user_id bigint NOT NULL CONSTRAINT fk_l_user_message_user REFERENCES STV202311131__DWH.h_users (hk_user_id), 
    hk_message_id bigint NOT NULL CONSTRAINT fk_l_user_message_message REFERENCES STV202311131__DWH.h_dialogs (hk_message_id), 
    load_dt datetime, 
    load_src varchar(20)
    )
    ORDER BY load_dt
    SEGMENTED BY hk_user_id ALL nodes
    PARTITION BY load_dt::date
    GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

    CREATE TABLE STV202311131__DWH.l_admins
    (
    hk_l_admin_id bigint PRIMARY KEY, 
    hk_user_id bigint NOT NULL CONSTRAINT fk_l_admins_user REFERENCES STV202311131__DWH.h_users (hk_user_id), 
    hk_group_id bigint NOT NULL CONSTRAINT fk_l_admin_groups REFERENCES STV202311131__DWH.h_groups (hk_group_id), 
    load_dt datetime, 
    load_src varchar(20)
    )
    ORDER BY load_dt
    SEGMENTED BY hk_l_admin_id ALL nodes
    PARTITION BY load_dt::date
    GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

    CREATE TABLE STV202311131__DWH.l_groups_dialogs
    (
    hk_l_groups_dialogs bigint PRIMARY KEY, 
    hk_message_id bigint NOT NULL CONSTRAINT fk_l_groups_dialogs_dialogs REFERENCES STV202311131__DWH.h_dialogs(hk_message_id), 
    hk_group_id bigint NOT NULL CONSTRAINT fk_l_groups_dialogs_groups REFERENCES STV202311131__DWH.h_groups (hk_group_id), 
    load_dt datetime, 
    load_src varchar(20)
    )
    ORDER BY load_dt
    SEGMENTED BY hk_l_groups_dialogs ALL nodes
    PARTITION BY load_dt::date
    GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
                        
    CREATE TABLE STV202311131__DWH.l_user_group_activity
        (hk_l_user_group_activity bigint PRIMARY KEY, 
        hk_user_id bigint NOT NULL CONSTRAINT fk_l_user_group_activity_users REFERENCES STV202311131__DWH.h_users (hk_user_id), 
        hk_group_id bigint NOT NULL CONSTRAINT fk_l_user_group_activity_groups REFERENCES STV202311131__DWH.h_groups (hk_group_id), 
        load_dt datetime, 
        load_src varchar(20)
    )
    ORDER BY load_dt
    SEGMENTED BY hk_user_id ALL nodes
    PARTITION BY load_dt::date
    GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2)                       
""")

def create_satellits():
    with vertica_python.connect(**vertica_conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute("""
    CREATE TABLE STV202311131__DWH.s_admins
    (
    hk_admin_id bigint NOT NULL CONSTRAINT fk_s_admins_l_admins REFERENCES STV202311131__DWH.l_admins (hk_l_admin_id), 
    is_admin boolean, 
    admin_from datetime, 
    load_dt datetime, 
    load_src varchar(20)
    )
    ORDER BY load_dt
    SEGMENTED BY hk_admin_id ALL nodes
    PARTITION BY load_dt::date
    GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
                                                
    CREATE TABLE STV202311131__DWH.s_group_name
    (
    hk_group_id bigint NOT NULL CONSTRAINT fk_s_group_name_l_admins REFERENCES STV202311131__DWH.h_groups (hk_group_id),
    group_name varchar(100),
    load_dt datetime,
    load_src varchar(20)
    )
    ORDER BY load_dt
    SEGMENTED BY hk_group_id ALL nodes
    PARTITION BY load_dt::date
    GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

    CREATE TABLE STV202311131__DWH.s_group_private_status
    (
    hk_group_id bigint NOT NULL CONSTRAINT fk_s_group_private_status_l_admins REFERENCES STV202311131__DWH.h_groups (hk_group_id), 
    is_private boolean, 
    load_dt datetime, 
    load_src varchar(20)
    )
    ORDER BY load_dt
    SEGMENTED BY hk_group_id ALL nodes
    PARTITION BY load_dt::date
    GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);                        

    CREATE TABLE STV202311131__DWH.s_dialog_info
    (
    hk_message_id bigint NOT NULL CONSTRAINT fk_s_dialog_info_h_dialog REFERENCES STV202311131__DWH.h_dialogs (hk_message_id), 
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
                        
    CREATE TABLE STV202311131__DWH.s_user_socdem
    (
    hk_user_id bigint NOT NULL CONSTRAINT fk_s_user_socdem_h_users REFERENCES STV202311131__DWH.h_users (hk_user_id), 
    country varchar(200), 
    age integer, 
    load_dt datetime, 
    load_src varchar(20)
    )
    ORDER BY load_dt
    SEGMENTED BY hk_user_id ALL nodes
    PARTITION BY load_dt::date
    GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

    CREATE TABLE STV202311131__DWH.s_user_chatinfo
    (
    hk_user_id bigint NOT NULL CONSTRAINT fk_s_user_chatinfo_h_users REFERENCES STV202311131__DWH.h_users (hk_user_id), 
    chat_name varchar(200), 
    load_dt datetime, 
    load_src varchar(20)
    )
    ORDER BY load_dt
    SEGMENTED BY hk_user_id ALL nodes
    PARTITION BY load_dt::date
    GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
                                        
    CREATE TABLE STV202311131__DWH.s_auth_history
        (
        hk_l_user_group_activity bigint NOT NULL CONSTRAINT fk_l_uga 
        REFERENCES STV202311131__DWH.l_user_group_activity (hk_l_user_group_activity), 
        user_id_from bigint, event varchar(200), 
        event_dt datetime, 
        load_dt datetime, 
        load_src varchar(20)
        )
    ORDER BY load_dt
    SEGMENTED BY hk_l_user_group_activity ALL nodes
    PARTITION BY load_dt::date
    GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);""")  

def fill_hubs():
    with vertica_python.connect(**vertica_conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute("""
    INSERT INTO STV202311131__DWH.h_users(hk_user_id, user_id, registration_dt, load_dt, load_src)
    SELECT hash(id) AS hk_user_id, id AS user_id, registration_dt, now() AS load_dt, 's3' AS load_src
    FROM STV202311131__STAGING.users
    WHERE hash(id) NOT IN (SELECT hk_user_id
    FROM STV202311131__DWH.h_users);

    INSERT INTO STV202311131__DWH.h_dialogs(hk_message_id, message_id, message_ts, load_dt, load_src)
    SELECT hash(message_id) AS hk_message_id, message_id AS message_id, message_ts, now() AS load_dt, 's3' AS load_src
    FROM STV202311131__STAGING.dialogs
    WHERE hash(message_id) NOT IN (SELECT hk_message_id
    FROM STV202311131__DWH.h_dialogs);

    INSERT INTO STV202311131__DWH.h_groups(hk_group_id, group_id, registration_dt, load_dt, load_src)
    SELECT hash(id) AS hk_group_id, id AS group_id, registration_dt, now() AS load_dt, 's3' AS load_src
    FROM STV202311131__STAGING.groups
    WHERE hash(id) NOT IN (SELECT hk_group_id
    FROM STV202311131__DWH.h_groups);
""")

def fill_links():
    with vertica_python.connect(**vertica_conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute("""
    INSERT INTO STV202311131__DWH.l_admins(hk_l_admin_id, hk_group_id, hk_user_id, load_dt, load_src)
    SELECT hash(hg.hk_group_id, hu.hk_user_id), hg.hk_group_id, hu.hk_user_id, now() AS load_dt, 's3' AS load_src
    FROM STV202311131__STAGING.groups AS g
    LEFT JOIN STV202311131__DWH.h_users AS hu ON
    g.admin_id = hu.user_id
    LEFT JOIN STV202311131__DWH.h_groups AS hg ON
    g.id = hg.group_id
    WHERE hash(hg.hk_group_id, hu.hk_user_id) NOT IN (SELECT hk_l_admin_id
    FROM STV202311131__DWH.l_admins);

    INSERT INTO STV202311131__DWH.l_groups_dialogs(hk_l_groups_dialogs, hk_message_id, hk_group_id, load_dt, load_src)
    SELECT hash(hg.hk_group_id, hd.hk_message_id), hd.hk_message_id, hg.hk_group_id, now() AS load_dt, 's3' AS load_src
    FROM STV202311131__STAGING.dialogs AS d
    JOIN STV202311131__DWH.h_dialogs AS hd ON
    d.message_id = hd.message_id
    JOIN STV202311131__DWH.h_groups AS hg ON
    d.message_group = hg.group_id
    WHERE hash(hg.hk_group_id, hd.hk_message_id) NOT IN (SELECT hk_l_groups_dialogs
    FROM STV202311131__DWH.l_groups_dialogs);

    INSERT INTO STV202311131__DWH.l_user_message (hk_l_user_message, hk_user_id, hk_message_id, load_dt, load_src)
    SELECT hash(hu.hk_user_id, hd.hk_message_id), hu.hk_user_id, hd.hk_message_id, now() AS load_dt, 's3' AS load_src
    FROM STV202311131__STAGING.dialogs AS d
    LEFT JOIN STV202311131__DWH.h_dialogs AS hd ON
    d.message_id = hd.message_id
    LEFT JOIN STV202311131__DWH.h_users AS hu ON
    d.message_from = hu.user_id
    WHERE hash(hu.hk_user_id, hd.hk_message_id) NOT IN (SELECT hk_l_user_message
    FROM STV202311131__DWH.l_user_message);
    
    INSERT INTO
        stv202311131__dwh.l_user_group_activity(hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
    SELECT DISTINCT
            hash(hg.hk_group_id, hu.hk_user_id), hu.hk_user_id, hg.hk_group_id, now() AS load_dt, 's3' AS load_src
    FROM stv202311131__staging.group_log AS gl
    LEFT JOIN stv202311131__dwh.h_users hu ON
        gl.user_id = hu.user_id
    LEFT JOIN stv202311131__dwh.h_groups hg ON
        gl.group_id = hg.group_id
    WHERE hash(hg.hk_group_id, hu.hk_user_id) NOT IN (
        SELECT hk_l_user_group_activity
    FROM stv202311131__dwh.l_user_group_activity)
    ;""") 

def fill_satellits():
    with vertica_python.connect(**vertica_conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute("""
    INSERT INTO STV202311131__DWH.s_admins(hk_admin_id, is_admin, admin_from, load_dt, load_src)
    SELECT la.hk_l_admin_id, TRUE AS is_admin, hg.registration_dt, now() AS load_dt, 's3' AS load_src
    FROM STV202311131__DWH.l_admins AS la
    LEFT JOIN STV202311131__DWH.h_groups AS hg ON
    la.hk_group_id = hg.hk_group_id;

    INSERT INTO STV202311131__DWH.s_group_name(hk_group_id, group_name, load_dt, load_src)
    SELECT hg.hk_group_id, g.group_name, now() AS load_dt, 's3' AS load_src
    FROM STV202311131__STAGING.groups AS g
    LEFT JOIN STV202311131__DWH.h_groups AS hg ON
    g.id = hg.group_id;

    INSERT INTO STV202311131__DWH.s_group_private_status(hk_group_id, is_private, load_dt, load_src)
    SELECT hg.hk_group_id, g.is_private, now() AS load_dt, 's3' AS load_src
    FROM STV202311131__STAGING.groups AS g
    LEFT JOIN STV202311131__DWH.h_groups AS hg ON
    g.id = hg.group_id;

    INSERT INTO STV202311131__DWH.s_dialog_info(hk_message_id, message, message_from, message_to, load_dt, load_src)
    SELECT hd.hk_message_id, d.message, d.message_from, d.message_to, now() AS load_dt, 's3' AS load_src
    FROM STV202311131__STAGING.dialogs AS d
    LEFT JOIN STV202311131__DWH.h_dialogs AS hd ON
    d.message_id = hd.message_id;                                                                                                

    INSERT INTO STV202311131__DWH.s_user_socdem(hk_user_id, country, age, load_dt, load_src)
    SELECT hu.hk_user_id, u.country, u.age, now() AS load_dt, 's3' AS load_src
    FROM STV202311131__STAGING.users AS u
    LEFT JOIN STV202311131__DWH.h_users AS hu ON
    u.id = hu.user_id;

    INSERT INTO STV202311131__DWH.s_user_chatinfo(hk_user_id, chat_name, load_dt, load_src)
    SELECT hu.hk_user_id, u.chat_name, now() AS load_dt, 's3' AS load_src
    FROM STV202311131__STAGING.users AS u
    LEFT JOIN STV202311131__DWH.h_users AS hu ON
    u.id = hu.user_id;
                            
    INSERT INTO STV202311131__DWH.s_auth_history(
            hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)
    SELECT hash(hg.hk_group_id, hu.hk_user_id), gl.user_id_from, gl.event, gl.datetime_ AS event_dt, now() AS load_dt, 's3' AS load_src
    FROM STV202311131__STAGING.group_log AS gl
    LEFT JOIN STV202311131__DWH.h_groups AS hg ON
    gl.group_id = hg.group_id
    LEFT JOIN STV202311131__DWH.h_users AS hu ON
    gl.user_id = hu.user_id
    LEFT JOIN STV202311131__DWH.l_user_group_activity AS luga ON 
    hg.hk_group_id = luga.hk_group_id
    AND hu.hk_user_id = luga.hk_user_id;
    """)             

with DAG(
        'sp6_project_dag_dds',
        schedule_interval=None,
        start_date=pendulum.parse('2022-07-13'),  
        catchup=False,                   
        tags=['sprint6', 'project', 'dds'],
) as dag:
    with TaskGroup("create_dds") as create_dds:
        create_hubs = PythonOperator(
            task_id='create_hubs',
            python_callable=create_hubs,
            dag=dag,
        )
        create_links = PythonOperator(
            task_id='create_links',
            python_callable=create_links,
            dag=dag,
        )
        create_satellits = PythonOperator(
            task_id='create_satellits',
            python_callable=create_satellits,
            dag=dag,
        )
        [create_hubs, create_links, create_satellits]

    with TaskGroup("fill_dds") as fill_dds:
        fill_hubs = PythonOperator(
            task_id='fill_hubs',
            python_callable=fill_hubs,
            dag=dag,
        )
        fill_links = PythonOperator(
            task_id='fill_links',
            python_callable=fill_links,
            dag=dag,
        )
        fill_satellits = PythonOperator(
            task_id='fill_satellits',
            python_callable=fill_satellits,
            dag=dag,
        )
        fill_hubs >> fill_links >> fill_satellits

    create_dds  >> fill_dds
