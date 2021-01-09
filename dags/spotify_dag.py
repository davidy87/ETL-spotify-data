"""
Reference: https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html#tutorial
"""

from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0, 0, 0, 0, 0),
    'email': ['dyoon0807@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'spotify_dag', 
    default_args=default_args, 
    description='DAG with ETL process for Spotify data',
    schedule_interval=timedelta(days=1)
)

run_etl = PythonOperator(
    task_id='spotify_etl',
    python_callable=None,
    dag=dag
)