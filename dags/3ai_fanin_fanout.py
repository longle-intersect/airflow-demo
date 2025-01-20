from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def process_part(part_id):
    # Placeholder function to simulate processing
    return f"Processed part {part_id}"

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('fan_in_fan_out_workflow', default_args=default_args, schedule_interval='@daily') as dag:
    start = DummyOperator(task_id='Start')
    end = DummyOperator(task_id='End')

    # Fan-Out: Create tasks to process parts simultaneously
    processing_tasks = [PythonOperator(
        task_id=f'process_part_{i}',
        python_callable=process_part,
        op_kwargs={'part_id': i}
    ) for i in range(1, 5)]  # assuming 4 parts to process

    # Fan-In: Aggregate results
    start >> processing_tasks >> end