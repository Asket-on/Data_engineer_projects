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

def create_stg_users():
    with vertica_python.connect(**vertica_conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE STV202311131__STAGING.users (
                id INT,
                chat_name VARCHAR(200),
                registration_dt TIMESTAMP,
                country VARCHAR(200),
                age INT,
                PRIMARY KEY (id),
                CHECK (LENGTH(chat_name) <= 200),
                CHECK (LENGTH(country) <= 200)
            )
            ORDER BY id;""")
            
def create_stg_groups():
    with vertica_python.connect(**vertica_conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE STV202311131__STAGING.groups (
                id INT,
                admin_id INT,
                group_name VARCHAR(100),
                registration_dt TIMESTAMP,
                is_private BOOLEAN,
                PRIMARY KEY (id),
                FOREIGN KEY (admin_id) REFERENCES STV202311131__STAGING.users(id),
                CHECK (LENGTH(group_name) <= 100)
            )
            ORDER BY id, admin_id
            PARTITION BY registration_dt::date
            GROUP BY calendar_hierarchy_day(registration_dt::date, 3, 2);""")

def create_stg_dialogs():
    with vertica_python.connect(**vertica_conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE STV202311131__STAGING.dialogs (
                message_id INT,
                message_ts TIMESTAMP,
                message_from INT,
                message_to INT,
                message VARCHAR(1000),
                message_group INT,
                PRIMARY KEY (message_id),
                FOREIGN KEY (message_from) REFERENCES STV202311131__STAGING.users(id),
                FOREIGN KEY (message_to) REFERENCES STV202311131__STAGING.users(id),
                FOREIGN KEY (message_group) REFERENCES STV202311131__STAGING.groups(id),
                CHECK (LENGTH(message) <= 1000)
            )
            ORDER BY message_id
            PARTITION BY message_ts::date
            GROUP BY calendar_hierarchy_day(message_ts::date, 3, 2)""")

def create_stg_group_log():
    with vertica_python.connect(**vertica_conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE STV202311131__STAGING.group_log (
                group_id INT,
                user_id INT,
                user_id_from INT,
                event VARCHAR(200),
                datetime_ TIMESTAMP,
                FOREIGN KEY (group_id) REFERENCES STV202311131__STAGING.groups(id),
                FOREIGN KEY (user_id) REFERENCES STV202311131__STAGING.users(id),
                CHECK (LENGTH(event) <= 200)
                )
                ORDER BY group_id
                PARTITION BY datetime_::date
                GROUP BY calendar_hierarchy_day(datetime_::date, 3, 2);""")

def fill_stg(table):
    with vertica_python.connect(**vertica_conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute(f"COPY STV202311131__STAGING.{table} FROM LOCAL '/data/{table}.csv' DELIMITER ',';" )


with DAG(
        'sp6_project_dag_stg',
        schedule_interval=None,
        start_date=pendulum.parse('2022-07-13'),  
        catchup=False,                   
        tags=['sprint6', 'project'],
) as dag:
    
    files = ['users', 'groups', 'dialogs', 'group_log']

    with TaskGroup("create_stg") as create_stg:
        create_stg_users = PythonOperator(
            task_id='create_stg_users',
            python_callable=create_stg_users,
        dag=dag)
        create_stg_groups = PythonOperator(
            task_id='create_stg_groups',
            python_callable=create_stg_groups,
        dag=dag)     
        create_stg_dialogs = PythonOperator(
            task_id='create_stg_dialogs',
            python_callable=create_stg_dialogs,
        dag=dag)
        create_stg_group_log = PythonOperator(
            task_id='create_stg_table',
            python_callable=create_stg_group_log,
        dag=dag)
        [create_stg_users, create_stg_groups, create_stg_dialogs, create_stg_group_log]

    with TaskGroup("fill_stage") as fill_stage:
        fill_users = PythonOperator(
            task_id=f'fill_stg_{files[0]}',
            python_callable=fill_stg,
            op_kwargs={'table': files[0]},
        dag=dag)
        fill_groups = PythonOperator(
            task_id=f'fill_stg_{files[1]}',
            python_callable=fill_stg,
            op_kwargs={'table': files[1]},
        dag=dag)
        fill_dialogs = PythonOperator(
            task_id=f'fill_stg_{files[2]}',
            python_callable=fill_stg,
            op_kwargs={'table': files[2]},
        dag=dag)       
        fill_group_log = PythonOperator(
            task_id=f'fill_stg_{files[3]}',
            python_callable=fill_stg,
            op_kwargs={'table': files[3]},
        dag=dag)
        fill_users >> fill_groups >> [fill_dialogs, fill_group_log]

    create_stg >> fill_stage
