from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    'slurm_job_submission',
    default_args=default_args,
    description='Submit SLURM job and fetch results',
    schedule_interval=None,
    catchup=False,
)

# SSH Hook
ssh_hook = SSHHook(ssh_conn_id="slurm_ssh_connection")

# Python function to create SLURM script
def create_slurm_script(**kwargs):
    script_content = """
    #!/bin/bash
    #SBATCH --job-name=test_job
    #SBATCH --output=job_output_%j.txt
    #SBATCH --ntasks=1
    #SBATCH --time=10:00
    echo "Running on host: $(hostname)"
    echo "Starting at: $(date)"
    sleep 10
    echo "Finishing at: $(date)"
    """
    script_path = '/tmp/slurm_job.sh'
    with open(script_path, 'w') as file:
        file.write(script_content)
    return script_path

create_script = PythonOperator(
    task_id='create_slurm_script',
    python_callable=create_slurm_script,
    provide_context=True,
    dag=dag,
)

# Task to send script to SLURM server
send_script = SSHOperator(
    task_id='send_script',
    ssh_hook=ssh_hook,
    command="scp -o StrictHostKeyChecking=no /tmp/slurm_job.sh lelong@sdclogin01.irs.environment.nsw.gov.au:/home/lelong/job_script",
    do_xcom_push=True,
    dag=dag,
)

# Task to submit the SLURM job
submit_job = SSHOperator(
    task_id='submit_job',
    ssh_hook=ssh_hook,
    command='sbatch /home/lelong/job_script/slurm_job.sh',
    do_xcom_push=True,
    dag=dag,
)

# Task to retrieve the output
retrieve_output = SSHOperator(
    task_id='retrieve_output',
    ssh_hook=ssh_hook,
    command='cat /home/lelong/job_script/job_output_$(squeue -u username --noheader | awk "{print $1}")',
    do_xcom_push=True,
    dag=dag,
)

# Set task dependencies
create_script >> send_script >> submit_job >> retrieve_output
