from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 1),
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    dag_id='bees_etl_pipeline',
    default_args=default_args,
    schedule_interval='0 4 * * *',
    catchup=False,
) as dag:

    task_bronze = BashOperator(
        task_id='coleta_api_bronze',
        bash_command='python3 /pipelines/breweries_api.py',
    )

    task_silver = BashOperator(
        task_id='bronze_to_silver',
        bash_command='python3 /pipelines/bronze_to_silver.py',
    )

    task_gold = BashOperator(
        task_id='silver_to_gold',
        bash_command='python3 /pipelines/silver_to_gold.py',
    )

    task_bronze >> task_silver >> task_gold