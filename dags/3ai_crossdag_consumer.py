from airflow import DAG
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2025, 1, 19),
}

# Define the DAG
with DAG(dag_id='3ai_consumer_dag',
         default_args=default_args,
         description='Consumer DAG that waits for producer',
         schedule_interval=None,
         start_date=days_ago(1),
         tags=['3ai', 'consumer', 'wait']) as consumer_dag:
    
    wait_for_producer = ExternalTaskSensor(
        task_id='wait_for_producer',
        external_dag_id='producer_dag',
        external_task_id='Load_Data',
        timeout=3600,
        poke_interval=30,
        mode='poke'
    )

    start_processing = DummyOperator(task_id='Start_Processing')
    end_processing = DummyOperator(task_id='End_Processing')

    wait_for_producer >> start_processing >> end_processing