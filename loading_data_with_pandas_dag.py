import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
from utils.using_pandas import run_process


default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}

with DAG(
    dag_id='loading_data_with_pandas',
    default_args=default_args,
    schedule_interval='0 */12 * * *'
) as dag:

    dummy_task = DummyOperator(task_id='dummy_task', retries=3)
    python_task = PythonOperator(task_id='python_task', python_callable=run_process)

    dummy_task >> python_task
