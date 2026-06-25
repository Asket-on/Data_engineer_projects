import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from datetime import date, datetime

# Set environment variables for Spark/Hadoop only if not running locally
if os.environ.get("ENV") != "local":
    os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
    os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
    os.environ['JAVA_HOME'] = '/usr'
    os.environ['SPARK_HOME'] = '/usr/lib/spark'
    os.environ['PYTHONPATH'] = '/usr/local/lib/python3.8'
else:
    if 'JAVA_HOME' not in os.environ:
        os.environ['JAVA_HOME'] = '/usr'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 1, 1),
}

base_data_dir = os.environ.get("BASE_DATA_DIR", "/user/mikvolobue/data")
events_path = f"{base_data_dir}/analytics/events/"
cities_path = f"{base_data_dir}/cities/geo"
today = date.today().strftime('%Y-%m-%d')

dag_spark = DAG(
    dag_id="datalake_project",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
)

mart_user_travel_info = SparkSubmitOperator(
    task_id='mart_user_travel_info',
    dag=dag_spark,
    application='/lessons/mart_user_travel_info.py',
    conn_id='yarn_spark',
    application_args=[
        events_path,
        cities_path,
        f"{base_data_dir}/mart/mart_user_travel_info"
    ],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores=2,
    executor_memory='2g'
)

mart_zones = SparkSubmitOperator(
    task_id='mart_zones',
    dag=dag_spark,
    application='/lessons/mart_zones.py',
    conn_id='yarn_spark',
    application_args=[
        events_path,
        cities_path,
        f"{base_data_dir}/mart/mart_zones"
    ],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores=2,
    executor_memory='2g'
)

mart_user_rec = SparkSubmitOperator(
    task_id='mart_user_rec',
    dag=dag_spark,
    application='/lessons/mart_user_rec.py',
    conn_id='yarn_spark',
    application_args=[
        events_path,
        cities_path,
        f"{base_data_dir}/mart/mart_user_rec",
        today
    ],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores=2,
    executor_memory='2g'
)

[mart_user_travel_info, mart_zones, mart_user_rec]
