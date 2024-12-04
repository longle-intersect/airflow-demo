from datetime import datetime

from airflow import DAG
from airflow.decorators import task, task_group
from datetime import datetime, timedelta

@task
def read_files_from_table():
    # Function to read the table and fetch the list of files to be processed
    files = ['file1.txt', 'file2.csv', 'file3.json', 'x_file.txt']
    return files


@task
def process_file(file_path):
    # Function to process each file
    print(f"Processing file: {file_path}")
    # TODO: Add code to process the file


# DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2024, 12, 1),
}


with DAG(dag_id='test_process_files', default_args=default_args, schedule_interval=None) as dag:
    @task_group
    def process_files_task_group(file_path):
        process_file(file_path)
    # expand method is used for the creation of dynamic tasks
    process_files_task_group.expand(file_path=read_files_from_table())