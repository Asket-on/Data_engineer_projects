import time
import requests
import json
import os
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook

postgres_conn_id = 'postgresql_de'
nickname = 'Asketr'
cohort = '4'

def get_http_headers():
    # Fetch connection parameters inside the execution context
    http_conn = HttpHook.get_connection('http_conn_id')
    api_key = http_conn.extra_dejson.get('api_key')
    return {
        'X-Nickname': nickname,
        'X-Cohort': cohort,
        'X-Project': 'True',
        'X-API-KEY': api_key,
        'Content-Type': 'application/x-www-form-urlencoded'
    }

def get_base_url():
    # Fetch host parameter inside the execution context
    http_conn = HttpHook.get_connection('http_conn_id')
    return http_conn.host

def generate_report(ti):
    base_url = get_base_url()
    headers = get_http_headers()
    print('Making request generate_report to:', base_url)

    response = requests.post(f'{base_url}/generate_report', headers=headers)
    response.raise_for_status()
    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)
    print(f'Response task_id is {task_id}')

def get_report(ti):
    base_url = get_base_url()
    headers = get_http_headers()
    task_id = ti.xcom_pull(key='task_id')
    print(f'Making request get_report for task_id: {task_id}')

    report_id = None

    for i in range(20):
        response = requests.get(f'{base_url}/get_report?task_id={task_id}', headers=headers)
        response.raise_for_status()
        print(f'Response is {response.content}')
        status = json.loads(response.content)['status']
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(10)

    if not report_id:
        raise TimeoutError("Report generation timed out.")

    ti.xcom_push(key='report_id', value=report_id)
    print(f'Report_id={report_id}')

def get_increment(date, ti):
    base_url = get_base_url()
    headers = get_http_headers()
    report_id = ti.xcom_pull(key='report_id')
    print(f'Making request get_increment for date: {date}, report_id: {report_id}')
    
    response = requests.get(
        f'{base_url}/get_increment?report_id={report_id}&date={str(date)}T00:00:00',
        headers=headers
    )
    response.raise_for_status()
    print(f'Response is {response.content}')

    increment_id = json.loads(response.content)['data']['increment_id']
    ti.xcom_push(key='increment_id', value=increment_id)
    print(f'increment_id={increment_id}')

def upload_data_to_staging(filename, date, pg_table, pg_schema, ti):
    base_url = get_base_url()
    increment_id = ti.xcom_pull(key='increment_id')
    
    # Download from our local mock storage path instead of yandexcloud
    s3_filename = f'{base_url}/storage/cohort_{cohort}/{nickname}/project/{increment_id}/{filename}'
    local_filename = f"/tmp/{date.replace('-', '')}_{filename}"
    
    print(f"Downloading increment file from {s3_filename} to {local_filename}...")
    response = requests.get(s3_filename)
    response.raise_for_status()
    
    with open(local_filename, "wb") as f:
        f.write(response.content)

    print(f"Loading {local_filename} into staging table {pg_schema}.{pg_table}...")
    df = pd.read_csv(local_filename)
    
    # Deduplicate staging insert
    df = df.drop_duplicates(subset=['id'])
    df = df.drop(columns=['id'])

    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)
    
    # Clean up local temporary file
    if os.path.exists(local_filename):
        os.remove(local_filename)

args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

business_dt = '{{ ds }}'

with DAG(
        'customer_retention',
        default_args=args,
        description='Demonstrate customer retention DWH pipeline with Airflow and Mock API',
        catchup=True,
        start_date=datetime.today() - timedelta(days=8),
        end_date=datetime.today() - timedelta(days=1),
        schedule_interval='@daily'
) as dag:

    # 1. Initialize staging and DWH schemas/calendar upon start
    init_schema = PostgresOperator(
        task_id='init_schema',
        postgres_conn_id=postgres_conn_id,
        sql="sql/init_schema.sql"
    )

    # 2. Extract tasks
    generate_report_task = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report
    )

    get_report_task = PythonOperator(
        task_id='get_report',
        python_callable=get_report
    )

    get_increment_task = PythonOperator(
        task_id='get_increment',
        python_callable=get_increment,
        op_kwargs={'date': business_dt}
    )

    # 3. Load staging task
    upload_user_order_inc = PythonOperator(
        task_id='upload_user_order_inc',
        python_callable=upload_data_to_staging,
        op_kwargs={
            'date': business_dt,
            'filename': 'user_orders_log_inc.csv',
            'pg_table': 'user_order_log',
            'pg_schema': 'staging'
        }
    )

    # 4. Load dimensions in parallel
    update_d_item_table = PostgresOperator(
        task_id='update_d_item',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_item.sql"
    )

    update_d_customer_table = PostgresOperator(
        task_id='update_d_customer',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_customer.sql"
    )

    update_d_city_table = PostgresOperator(
        task_id='update_d_city',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_city.sql"
    )

    # 5. Load facts and analytical marts
    update_f_sales = PostgresOperator(
        task_id='update_f_sales',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_sales.sql",
        parameters={"date": {business_dt}}
    )

    update_f_customer_retention = PostgresOperator(
        task_id='update_f_customer_retention',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_customer_retention.sql",
        parameters={"date": {business_dt}}
    )

    # Dependency lineage mapping
    (
        init_schema
        >> generate_report_task
        >> get_report_task
        >> get_increment_task
        >> upload_user_order_inc
        >> [update_d_item_table, update_d_city_table, update_d_customer_table]
        >> update_f_sales
        >> update_f_customer_retention
    )