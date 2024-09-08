from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
import pendulum
import boto3
import vertica_python
import json


AWS_ACCESS_KEY_ID = Variable.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY')
conn_info = BaseHook.get_connection('vertika_conn')
# Assuming "extra" field contains a valid JSON string
extra_dict = json.loads(conn_info.extra)
# Create a dictionary with connection information
vertica_conn_info = {
    'host': extra_dict.get("host"),
    'port': extra_dict.get("port"),
    'user': extra_dict.get("user"),
    'password': extra_dict.get("password"),
    'database': extra_dict.get("database"),
    'autocommit': extra_dict.get("autocommit"),
}

def fetch_s3_file(bucket: str, key: str):

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
 
    s3_client.download_file(
        Bucket=bucket,
        Key=f'{key}.csv',
        Filename=f'/data/{key}.csv'
    )

@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def sp6_project_dag_get_data_s3():
    files = ['users', 'groups', 'dialogs', 'group_log']
    
    users = PythonOperator(
        task_id=f'fetch_{files[0]}.csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': files[0]},
    )
    groups = PythonOperator(
        task_id=f'fetch_{files[1]}.csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': files[1]},
    )
    dialogs = PythonOperator(
        task_id=f'fetch_{files[2]}.csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': files[2]},
    )
    
    group_log = PythonOperator(
        task_id=f'fetch_{files[3]}.csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': files[3]},
    )

    users >> groups >> [dialogs, group_log]

dag_instance = sp6_project_dag_get_data_s3() 