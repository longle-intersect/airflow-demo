from datetime import datetime
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG, its scheduling, and set catchup to False
dag = DAG(
    'test_slurm_integration',
    default_args=default_args,
    description='A simple DAG to test SSH connection and submit a SLURM job',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# Define the task to test SSH connection
test_ssh_connection = SSHOperator(
    task_id='test_ssh_connection',
    ssh_conn_id='slurm_ssh_connection',
    command='echo "SSH connection successful!"',
    dag=dag,
)

# Define the task to submit a SLURM job
submit_slurm_job = SSHOperator(
    task_id='submit_slurm_job',
    ssh_conn_id='slurm_ssh_connection',
    command='sbatch /home/lelong/job_script/sample_slurm.slurm',
    dag=dag,
)

# Set the task dependencies
test_ssh_connection >> submit_slurm_job
