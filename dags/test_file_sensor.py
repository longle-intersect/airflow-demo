from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sensors.filesystem import FileSensor

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# Initialize the DAG
dag = DAG(
    'slurm_job_with_file_sensor',
    default_args=default_args,
    description='Submit SLURM job and wait for output file',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
)

# Task to wait for the output file to be created
wait_for_file = FileSensor(
    task_id='wait_for_output_file',
    filepath='/path/to/job_output_file',
    fs_conn_id='fs_default',  # Configure this connection to point to the appropriate file system
    timeout=6000,  # Seconds you want the sensor to wait for the file to appear
    poke_interval=60,  # Seconds between tries
    dag=dag
)

# Task to retrieve the output
retrieve_output = SSHOperator(
    task_id='retrieve_output',
    ssh_conn_id='slurm_ssh_connection',
    command='cat /path/to/job_output_file',
    do_xcom_push=True,
    dag=dag,
)

# Set task dependencies
wait_for_file >> retrieve_output
