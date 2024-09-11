from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.vertica.hooks.vertica import VerticaHook
from data_transfer import DataTransfer


# Connections
pg_conn = PostgresHook('postgres_conn').get_conn()
vertica_hook = VerticaHook('vertica_conn').get_conn()


# Defining DAG parameters
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 10, 1),
    'end_date': datetime(2022, 11, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dt = DataTransfer(pg_conn, vertica_hook)

def export_to_csv(table_name, date):
    dt.export_data_to_csv(table_name, date)

def import_to_vertica(table_name, date):
    dt.import_data_to_vertica(table_name, date)


with DAG(
        'postgres_to_vertica',
        default_args=default_args,
        description='DAG for transferring data from PostgreSQL to Vertica',
        catchup=True,
        schedule_interval='@daily',
) as dag:
    tables = ['transactions', 'currencies']
    for table in tables:
        export_task = PythonOperator(
            task_id=f'export_{table}',
            python_callable=export_to_csv,
            op_kwargs={'table_name': table, 'date': '{{ ds }}'},
            dag=dag,
        )

        import_task = PythonOperator(
            task_id=f'import_{table}',
            python_callable=import_to_vertica,
            op_kwargs={'table_name': table, 'date': '{{ ds }}'},
            dag=dag,
        )

        export_task >> import_task 