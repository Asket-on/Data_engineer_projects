import os
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

def fetch_s3_file(bucket: str, key: str):
    from airflow.models import Variable
    from airflow.hooks.base import BaseHook
    import boto3

    # Fetch connection parameters inside execution callable to prevent scheduler latency
    try:
        s3_conn = BaseHook.get_connection('s3_conn')
        aws_access_key = s3_conn.login
        aws_secret_key = s3_conn.password
        endpoint_url = s3_conn.host
    except Exception:
        aws_access_key = Variable.get('AWS_ACCESS_KEY_ID', default_var=None)
        aws_secret_key = Variable.get('AWS_SECRET_ACCESS_KEY', default_var=None)
        endpoint_url = 'https://storage.yandexcloud.net'

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url=endpoint_url or 'https://storage.yandexcloud.net',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
    )
 
    os.makedirs('/data', exist_ok=True)
    local_path = f'/data/{key}.csv'
    
    print(f"Downloading {key}.csv from bucket {bucket} to {local_path} (S3: {endpoint_url})...")
    s3_client.download_file(
        Bucket=bucket,
        Key=f'{key}.csv',
        Filename=local_path
    )
    print("Download completed successfully.")

with DAG(
    dag_id='sp6_project_dag_get_data_s3',
    schedule_interval=None,
    start_date=pendulum.parse('2022-07-13'),
    catchup=False,
    tags=['sprint6', 'project']
) as dag:
    
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