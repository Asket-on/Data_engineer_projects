from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from data_transfer import DataTransfer


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

def init_schema():
    dt = DataTransfer()
    dt.run_ddl_script(force_recreate=False)

def export_to_csv(table_name, date):
    dt = DataTransfer()
    dt.export_data_to_csv(table_name, date)

def import_to_vertica(table_name, date):
    dt = DataTransfer()
    dt.import_data_to_vertica(table_name, date)


with DAG(
        'postgres_to_vertica',
        default_args=default_args,
        description='DAG for transferring data from PostgreSQL to Vertica',
        catchup=True,
        schedule_interval='@daily',
) as dag:
    
    init_schema_task = PythonOperator(
        task_id='init_vertica_schema',
        python_callable=init_schema,
        dag=dag,
    )

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

        init_schema_task >> export_task >> import_task 