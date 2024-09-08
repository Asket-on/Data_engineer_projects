import logging

import pendulum
from datetime import datetime, timedelta
import pandas as pd
import requests
import json
import psycopg2 
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup

log = logging.getLogger(__name__)
headers={
    "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f",
    "X-Nickname": "asketr",
    "X-Cohort": "4" 
    }
URL = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/'

def get_api_to_stg(pg_conn, collection_name):
    offset = 0
    data = []
    seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
    
    while True:
        url = URL+collection_name+f"/?sort_field='_id'&sort_direction='asc'&limit=50&offset={offset}"
        if collection_name == 'deliveries':
            url = URL+collection_name+f"/?sort_field='_id'&sort_direction='asc'&limit=50&offset={offset}&from={seven_days_ago}"

        
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        page = json.loads(r.content)
        if not page:
            break
        data.extend(page)
        offset += 50
           
    i = 0

    for val in data:
        if collection_name == 'deliveries':
            id = val['order_id']         
        else:
            id = val['_id']
        val = str(val)
        with psycopg2.connect(pg_conn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO stg.deliverysystem_{collection_name}(object_id, object_value)
                    VALUES (%(id)s, %(val)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET object_value = EXCLUDED.object_value;
                """.format(collection_name=collection_name),
                {
                    "id": id,
                    "val": val
                }
            )
                conn.commit()

def update_couriers():
    with psycopg2.connect(pg_conn.get_uri()) as conn:
        with conn.cursor() as cur:
            cur.execute(
            """
            SELECT *
            FROM stg.deliverysystem_couriers
            ;
            """
        )
            objs = cur.fetchall()

            for val in objs:
                val = eval(val[2])
                cur.execute(
                """
                    INSERT INTO dds.dm_api_couriers(courier_id, courier_name)
                    VALUES (%(id)s, %(val)s)
                    ON CONFLICT (courier_id) DO UPDATE
                    SET courier_name = EXCLUDED.courier_name;
                """,
                {
                    "id": val['_id'],
                    "val": val['name']
                }
            )
                conn.commit()

def update_orders():
    with psycopg2.connect(pg_conn.get_uri()) as conn:
        with conn.cursor() as cur:
            cur.execute(
            """
            SELECT object_value
            FROM stg.deliverysystem_deliveries
            ;
            """
        )
            objs = cur.fetchall()

            for val in objs:
                val = eval(val[0])
                cur.execute(
                """
                    INSERT INTO dds.dm_api_orders(order_id, order_ts)
                    VALUES (%(id)s, %(val)s)
                    ON CONFLICT (order_id) DO UPDATE
                    SET order_ts = EXCLUDED.order_ts;
                """,
                {
                    "id": val['order_id'],
                    "val": val['order_ts']
                }
            )
                conn.commit()

def update_delivery_details():
    with psycopg2.connect(pg_conn.get_uri()) as conn:
        with conn.cursor() as cur:
            cur.execute(
            """
            SELECT object_value
            FROM stg.deliverysystem_deliveries
            ;
            """
        )
            objs = cur.fetchall()

            for val in objs:
                val = eval(val[0])
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
                    "delivery_id": val['delivery_id'],
                    "courier_id": val['courier_id'],
                    "delivery_ts": val['delivery_ts'],
                    "rate": val['rate']
                }
            )
                conn.commit()

def update_fct_api_sales():
    with psycopg2.connect(pg_conn.get_uri()) as conn:
        with conn.cursor() as cur:
            cur.execute(
            """
            SELECT object_value
            FROM stg.deliverysystem_deliveries
            ;
            """
        )
            objs = cur.fetchall()

            for val in objs:
                val = eval(val[0])
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
                    "order_id": val['order_id'],
                    "delivery_id": val['delivery_id'],
                    "order_sum": val['sum'],
                    "tip_sum": val['tip_sum']
                }
            )
                conn.commit()

with DAG(
    dag_id="sp5_project", 
    start_date=datetime(2022, 1, 1),
    schedule_interval='0/15 * * * *',
    is_paused_upon_creation=False,
    catchup=False,) as dag:

    pg_conn = BaseHook.get_connection('PG_WAREHOUSE_CONNECTION')

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
            op_kwargs={'pg_conn':pg_conn.get_uri(),'collection_name':'couriers'},
            dag=dag)
        
        stg_deliveries = PythonOperator(
            task_id='get_api_to_stg_deliveries',
            python_callable=get_api_to_stg,
            op_kwargs={'pg_conn':pg_conn.get_uri(),'collection_name':'deliveries'},
            dag=dag)
        [stg_couriers, stg_deliveries]

    with TaskGroup("DDS") as dds:
        dds_couriers = PythonOperator(
            task_id='dds_couriers',
            python_callable=update_couriers,
            dag=dag)
        
        dds_orders = PythonOperator(
            task_id='dds_orders',
            python_callable=update_orders,
            dag=dag)

        dds_delivery_details = PythonOperator(
            task_id='dds_delivery_details',
            python_callable=update_delivery_details,
            dag=dag)

        dds_fct_api_sales = PythonOperator(
            task_id='dds_fct_api_sales',
            python_callable=update_fct_api_sales,
            dag=dag)

        [dds_orders, dds_couriers] >> dds_delivery_details >> dds_fct_api_sales
    cdm_dm_courier_ledger = PostgresOperator(
        task_id='cdm_courier_ledger',
        sql="update_cdm_dm_courier_ledger.sql",
        postgres_conn_id='PG_WAREHOUSE_CONNECTION'
    )

    create_table >> stage >> dds >> cdm_dm_courier_ledger
