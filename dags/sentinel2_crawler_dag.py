from datetime import datetime
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import sys
import base64

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'sdc_sentinel2_crawler',
    default_args=default_args,
    description='Daily Sentinel-2 folder crawler via SSH',
    schedule_interval=None,  # Run daily at midnight
    start_date=datetime(2025, 3, 19),
    catchup=False,
)

# Function to parse XCom and push separate values
def parse_xcom_and_push(**kwargs):
    ti = kwargs['ti']
    xcom_value = ti.xcom_pull(task_ids='crawl_next_sentinel2_folder')
    if xcom_value:
        # Decode base64 if necessary
        try:
            decoded_value = base64.b64decode(xcom_value).decode('utf-8') # if isinstance(xcom_value, bytes) else xcom_value
        except Exception as e:
            print(f"Error decoding base64: {e}", file=sys.stderr)
            decoded_value = xcom_value

        print(f"Decoded value: {decoded_value}", file=sys.stdout)  

        # Split lines
        lines = decoded_value.split('\n')
        num_files = None
        folder_visited = None
        for line in lines:
            if line.startswith('XCOM_NUM_FILES:'):
                num_files = int(line.replace('XCOM_NUM_FILES:', '').strip())
            elif line.startswith('XCOM_FOLDER_VISITED:'):
                folder_visited = line.replace('XCOM_FOLDER_VISITED:', '').strip()
        
        if num_files is not None:
            ti.xcom_push(key='num_files', value=num_files)
            print(f"Pushed num_files: {num_files}", file=sys.stdout)
        if folder_visited is not None:
            ti.xcom_push(key='folder_visited', value=folder_visited)
            print(f"Pushed folder_visited: {folder_visited}", file=sys.stdout)
        else:
            print(f"Warning: Could not parse XCom values from {decoded_value}", file=sys.stderr)

# Define the SSH command
ssh_command = (
    "cd rswms/dataops/sdc_slurm && "
    "python sentinel2crawler.py 202301 202312 output_files "
)

# Task to crawl via SSH
crawl_task = SSHOperator(
    task_id='crawl_next_sentinel2_folder',
    ssh_conn_id='slurm_ssh_connection',
    command=ssh_command,
    dag=dag,
    do_xcom_push=True,
)

# Task to parse and push XCom values
parse_xcom_task = PythonOperator(
    task_id='parse_xcom_values',
    python_callable=parse_xcom_and_push,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
crawl_task >> parse_xcom_task