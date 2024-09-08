from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.vertica.hooks.vertica import VerticaHook
from data_transfer import DataTransfer


# Connections
pg_conn = PostgresHook('postgres_conn').get_conn()
vertica_hook = VerticaHook('vertica_conn').get_conn()


# Определение параметров DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 10, 1),
    'end_date': datetime(2022, 11, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
#    'retry_delay': timedelta(minutes=10),
}

dt = DataTransfer(pg_conn, vertica_hook)

def insert_global_metrics(date):
    dt.insert_into_global_metrics(date)

with DAG(
        'insert_cdm_vertica',
        default_args=default_args,
        description='DAG for insert data cdm to Vertica',
        catchup=True,
        schedule_interval='@daily',
) as dag:
    insert_into_global_metrics = PythonOperator(
        task_id=f'insert_into_global_metrics',
        python_callable=insert_global_metrics,
        op_kwargs={'date': '{{ ds }}'},
        dag=dag,
    )

    insert_into_global_metrics