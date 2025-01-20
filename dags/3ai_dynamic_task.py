from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os

# Function to list data files
def list_data_files():
    return os.listdir('/path/to/data/files')

# Function to process a single data file
def process_file(file_name):
    # Processing logic here
    pass

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG('dynamic_file_processing', default_args=default_args, schedule_interval='@daily') as dag:
    start = PythonOperator(task_id='Start', python_callable=lambda: print("Workflow started"))

    # Dynamically create a task for each file
    for file in list_data_files():
        task = PythonOperator(
            task_id=f'process_{file}',
            python_callable=process_file,
            op_kwargs={'file_name': file}
        )
        start >> task

    end = PythonOperator(task_id='End', python_callable=lambda: print("Workflow completed"))