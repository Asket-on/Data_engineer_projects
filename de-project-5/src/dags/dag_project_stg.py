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

def create_stg_users():
    import vertica_python
    conn_info = get_vertica_conn_info()
    prefix = get_schema_prefix()
    
    with vertica_python.connect(**conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {prefix}__STAGING;")
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {prefix}__STAGING.users (
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
    import vertica_python
    conn_info = get_vertica_conn_info()
    prefix = get_schema_prefix()
    
    with vertica_python.connect(**conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {prefix}__STAGING.groups (
                id INT,
                admin_id INT,
                group_name VARCHAR(100),
                registration_dt TIMESTAMP,
                is_private BOOLEAN,
                PRIMARY KEY (id),
                FOREIGN KEY (admin_id) REFERENCES {prefix}__STAGING.users(id),
                CHECK (LENGTH(group_name) <= 100)
            )
            ORDER BY id, admin_id
            PARTITION BY registration_dt::date
            GROUP BY calendar_hierarchy_day(registration_dt::date, 3, 2);""")

def create_stg_dialogs():
    import vertica_python
    conn_info = get_vertica_conn_info()
    prefix = get_schema_prefix()
    
    with vertica_python.connect(**conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {prefix}__STAGING.dialogs (
                message_id INT,
                message_ts TIMESTAMP,
                message_from INT,
                message_to INT,
                message VARCHAR(1000),
                message_group INT,
                PRIMARY KEY (message_id),
                FOREIGN KEY (message_from) REFERENCES {prefix}__STAGING.users(id),
                FOREIGN KEY (message_to) REFERENCES {prefix}__STAGING.users(id),
                FOREIGN KEY (message_group) REFERENCES {prefix}__STAGING.groups(id),
                CHECK (LENGTH(message) <= 1000)
            )
            ORDER BY message_id
            PARTITION BY message_ts::date
            GROUP BY calendar_hierarchy_day(message_ts::date, 3, 2)""")

def create_stg_group_log():
    import vertica_python
    conn_info = get_vertica_conn_info()
    prefix = get_schema_prefix()
    
    with vertica_python.connect(**conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {prefix}__STAGING.group_log (
                group_id INT,
                user_id INT,
                user_id_from INT,
                event VARCHAR(200),
                datetime_ TIMESTAMP,
                FOREIGN KEY (group_id) REFERENCES {prefix}__STAGING.groups(id),
                FOREIGN KEY (user_id) REFERENCES {prefix}__STAGING.users(id),
                CHECK (LENGTH(event) <= 200)
            )
            ORDER BY group_id
            PARTITION BY datetime_::date
            GROUP BY calendar_hierarchy_day(datetime_::date, 3, 2);""")

def fill_stg(table):
    import vertica_python
    conn_info = get_vertica_conn_info()
    prefix = get_schema_prefix()
    
    with vertica_python.connect(**conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {prefix}__STAGING.{table};")
            cur.execute(f"COPY {prefix}__STAGING.{table} FROM LOCAL '/data/{table}.csv' DELIMITER ',';")

with DAG(
    dag_id='sp6_project_dag_stg',
    schedule_interval=None,
    start_date=pendulum.parse('2022-07-13'),  
    catchup=False,                   
    tags=['sprint6', 'project', 'stg'],
) as dag:
    
    files = ['users', 'groups', 'dialogs', 'group_log']

    with TaskGroup("create_stg") as create_stg:
        t_create_users = PythonOperator(
            task_id='create_stg_users',
            python_callable=create_stg_users,
        )
        t_create_groups = PythonOperator(
            task_id='create_stg_groups',
            python_callable=create_stg_groups,
        )     
        t_create_dialogs = PythonOperator(
            task_id='create_stg_dialogs',
            python_callable=create_stg_dialogs,
        )
        t_create_group_log = PythonOperator(
            task_id='create_stg_group_log',
            python_callable=create_stg_group_log,
        )
        
        t_create_users >> t_create_groups >> [t_create_dialogs, t_create_group_log]

    with TaskGroup("fill_stage") as fill_stage:
        fill_users = PythonOperator(
            task_id=f'fill_stg_{files[0]}',
            python_callable=fill_stg,
            op_kwargs={'table': files[0]},
        )
        fill_groups = PythonOperator(
            task_id=f'fill_stg_{files[1]}',
            python_callable=fill_stg,
            op_kwargs={'table': files[1]},
        )
        fill_dialogs = PythonOperator(
            task_id=f'fill_stg_{files[2]}',
            python_callable=fill_stg,
            op_kwargs={'table': files[2]},
        )       
        fill_group_log = PythonOperator(
            task_id=f'fill_stg_{files[3]}',
            python_callable=fill_stg,
            op_kwargs={'table': files[3]},
        )
        
        fill_users >> fill_groups >> [fill_dialogs, fill_group_log]

    create_stg >> fill_stage
