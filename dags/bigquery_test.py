from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryGetDatasetOperator,
    BigQueryUpdateDatasetOperator,
)
from datetime import datetime, timedelta

default_args = {
    'owner': 'admin',
    'retry': 5,
    'retry_delay': timedelta(minutes=5)
}

def get_bg():
    print("hello world")

with DAG(
    default_args=default_args,
    dag_id='test_bg_3',
    start_date=datetime(2022, 10, 11),
    schedule_interval='@daily'
) as dag:

    create_dataset = BigQueryCreateEmptyDatasetOperator(task_id="create_dataset", dataset_id='test_2')

    create_dataset