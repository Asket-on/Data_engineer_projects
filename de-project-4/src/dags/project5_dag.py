import logging
import json
import psycopg2
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

log = logging.getLogger(__name__)

def parse_object_value(val_str):
    import ast
    try:
        return json.loads(val_str)
    except (json.JSONDecodeError, TypeError):
        return ast.literal_eval(val_str)

def get_api_to_stg(collection_name):
    from airflow.hooks.base import BaseHook
    
    # 1. Fetch connection details dynamically within the task execution
    pg_conn_uri = BaseHook.get_connection('PG_WAREHOUSE_CONNECTION').get_uri()
    api_conn = BaseHook.get_connection('delivery_api_conn')
    
    url_base = api_conn.host or 'http://mock-api:5001'
    if not url_base.endswith('/'):
        url_base += '/'
        
    api_key = api_conn.extra_dejson.get('api_key', '25c27781-8fde-4b30-a22e-524044a7580f')
    
    headers = {
        "X-API-KEY": api_key,
        "X-Nickname": "asketr",
        "X-Cohort": "4"
    }

    offset = 0
    data = []
    seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
    
    # 2. Query API with pagination
    while True:
        url = f"{url_base}{collection_name}/?sort_field='_id'&sort_direction='asc'&limit=50&offset={offset}"
        if collection_name == 'deliveries':
            url = f"{url_base}{collection_name}/?sort_field='_id'&sort_direction='asc'&limit=50&offset={offset}&from={seven_days_ago}"

        log.info(f"Fetching: {url}")
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        page = r.json()
        if not page:
            break
        data.extend(page)
        offset += 50
           
    log.info(f"Fetched {len(data)} items from {collection_name}. Loading into staging...")

    # 3. Single connection session for batch loading (resolves N+1 connection leak)
    with psycopg2.connect(pg_conn_uri) as conn:
        with conn.cursor() as cur:
            for val in data:
                if collection_name == 'deliveries':
                    item_id = val['order_id']         
                else:
                    item_id = val['_id']
                
                # Store as standard JSON format
                val_str = json.dumps(val)
                
                cur.execute(
                    """
                    INSERT INTO stg.deliverysystem_{collection_name}(object_id, object_value)
                    VALUES (%(id)s, %(val)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET object_value = EXCLUDED.object_value;
                    """.format(collection_name=collection_name),
                    {
                        "id": item_id,
                        "val": val_str
                    }
                )
            conn.commit()
    log.info(f"Successfully loaded {len(data)} records into stg.deliverysystem_{collection_name}.")

def update_couriers():
    from airflow.hooks.base import BaseHook
    pg_conn_uri = BaseHook.get_connection('PG_WAREHOUSE_CONNECTION').get_uri()
    
    with psycopg2.connect(pg_conn_uri) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM stg.deliverysystem_couriers;")
            objs = cur.fetchall()

            for val in objs:
                parsed_val = parse_object_value(val[2])
                cur.execute(
                    """
                    INSERT INTO dds.dm_api_couriers(courier_id, courier_name)
                    VALUES (%(id)s, %(val)s)
                    ON CONFLICT (courier_id) DO UPDATE
                    SET courier_name = EXCLUDED.courier_name;
                    """,
                    {
                        "id": parsed_val['_id'],
                        "val": parsed_val['name']
                    }
                )
            conn.commit()

def update_orders():
    from airflow.hooks.base import BaseHook
    pg_conn_uri = BaseHook.get_connection('PG_WAREHOUSE_CONNECTION').get_uri()
    
    with psycopg2.connect(pg_conn_uri) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT object_value FROM stg.deliverysystem_deliveries;")
            objs = cur.fetchall()

            for val in objs:
                parsed_val = parse_object_value(val[0])
                cur.execute(
                    """
                    INSERT INTO dds.dm_api_orders(order_id, order_ts)
                    VALUES (%(id)s, %(val)s)
                    ON CONFLICT (order_id) DO UPDATE
                    SET order_ts = EXCLUDED.order_ts;
                    """,
                    {
                        "id": parsed_val['order_id'],
                        "val": parsed_val['order_ts']
                    }
                )
            conn.commit()

def update_delivery_details():
    from airflow.hooks.base import BaseHook
    pg_conn_uri = BaseHook.get_connection('PG_WAREHOUSE_CONNECTION').get_uri()
    
    with psycopg2.connect(pg_conn_uri) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT object_value FROM stg.deliverysystem_deliveries;")
            objs = cur.fetchall()

            for val in objs:
                parsed_val = parse_object_value(val[0])
                cur.execute(
                    """
                    INSERT INTO dds.dm_api_delivery_details(delivery_id, courier_id, delivery_ts, rate)
                    VALUES (%(delivery_id)s, %(courier_id)s, %(delivery_ts)s, %(rate)s)
                    ON CONFLICT (delivery_id) DO UPDATE
                    SET courier_id = EXCLUDED.courier_id,
                        delivery_ts = EXCLUDED.delivery_ts,
                        rate = EXCLUDED.rate;
                    """,
                    {
                        "delivery_id": parsed_val['delivery_id'],
                        "courier_id": parsed_val['courier_id'],
                        "delivery_ts": parsed_val['delivery_ts'],
                        "rate": parsed_val['rate']
                    }
                )
            conn.commit()

def update_fct_api_sales():
    from airflow.hooks.base import BaseHook
    pg_conn_uri = BaseHook.get_connection('PG_WAREHOUSE_CONNECTION').get_uri()
    
    with psycopg2.connect(pg_conn_uri) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT object_value FROM stg.deliverysystem_deliveries;")
            objs = cur.fetchall()

            for val in objs:
                parsed_val = parse_object_value(val[0])
                cur.execute(
                    """
                    INSERT INTO dds.fct_api_sales(order_id, delivery_id, order_sum, tip_sum)
                    VALUES (%(order_id)s, %(delivery_id)s, %(order_sum)s, %(tip_sum)s)
                    ON CONFLICT (order_id) DO UPDATE
                    SET delivery_id = EXCLUDED.delivery_id,
                        order_sum = EXCLUDED.order_sum,
                        tip_sum = EXCLUDED.tip_sum;
                    """,
                    {
                        "order_id": parsed_val['order_id'],
                        "delivery_id": parsed_val['delivery_id'],
                        "order_sum": parsed_val['sum'],
                        "tip_sum": parsed_val['tip_sum']
                    }
                )
            conn.commit()

with DAG(
    dag_id="sp5_project", 
    start_date=datetime(2022, 1, 1),
    schedule_interval='0/15 * * * *',
    is_paused_upon_creation=False,
    catchup=False,
) as dag:

    # создание таблиц
    create_table = PostgresOperator(
        task_id='create_table',
        sql="init_db.sql",
        postgres_conn_id='PG_WAREHOUSE_CONNECTION'
    )

    with TaskGroup("STG") as stage:
        stg_couriers = PythonOperator(
            task_id='get_api_to_stg_couriers',
            python_callable=get_api_to_stg,
            op_kwargs={'collection_name': 'couriers'},
            dag=dag
        )
        
        stg_deliveries = PythonOperator(
            task_id='get_api_to_stg_deliveries',
            python_callable=get_api_to_stg,
            op_kwargs={'collection_name': 'deliveries'},
            dag=dag
        )
        [stg_couriers, stg_deliveries]

    with TaskGroup("DDS") as dds:
        dds_couriers = PythonOperator(
            task_id='dds_couriers',
            python_callable=update_couriers,
            dag=dag
        )
        
        dds_orders = PythonOperator(
            task_id='dds_orders',
            python_callable=update_orders,
            dag=dag
        )

        dds_delivery_details = PythonOperator(
            task_id='dds_delivery_details',
            python_callable=update_delivery_details,
            dag=dag
        )

        dds_fct_api_sales = PythonOperator(
            task_id='dds_fct_api_sales',
            python_callable=update_fct_api_sales,
            dag=dag
        )

        [dds_orders, dds_couriers] >> dds_delivery_details >> dds_fct_api_sales

    cdm_dm_courier_ledger = PostgresOperator(
        task_id='cdm_courier_ledger',
        sql="update_cdm_dm_courier_ledger.sql",
        postgres_conn_id='PG_WAREHOUSE_CONNECTION'
    )

    create_table >> stage >> dds >> cdm_dm_courier_ledger
