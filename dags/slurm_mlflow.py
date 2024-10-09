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
    'slurm_mlflow',
    default_args=default_args,
    description='Submit SLURM job and fetch results',
    schedule_interval=None,
    catchup=False,
)


# SSH Hook

ssh_hook = SSHHook(ssh_conn_id="yueyang")


# Python function to create SLURM script

def create_slurm_script(**kwargs):
    script_content = """#!/bin/bash
#SBATCH --job-name=test_mlflow_job
#SBATCH --output=./job_output_test.txt
#SBATCH -n 1
#SBATCH --mem=1000M
#SBATCH -t 00:20:00
# Specify the work to be done
module unload python
module unload RSstandard
. ~/venv/mlflow/bin/activate
python train_diabetes.py
deactivate
"""
    script_path = '/home/airflow/slurm_scripts/slurm_mlflow_script.slurm'
    with open(script_path, 'w') as file:
        file.write(script_content)
    return script_path


def log_scp_command():
    import subprocess
    command = "scp -o StrictHostKeyChecking=no /home/airflow/slurm_scripts/slurm_mlflow_script.slurm liuyuey@sdclogin01.irs.environment.nsw.gov.au:/home/liuyuey/job_script"
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = process.communicate()
    print("STDOUT:", out)
    print("STDERR:", err)


create_script = PythonOperator(
    task_id='create_slurm_script',
    python_callable=create_slurm_script,
    provide_context=True,
    dag=dag,
)


# Task to send script to SLURM server
#send_script = SSHOperator(
#    task_id='send_script',
#    ssh_hook=ssh_hook,
#    command="scp -o StrictHostKeyChecking=no /home/airflow/slurm_scripts/slurm_job.sh lelong@sdclogin01.irs.environment.nsw.gov.au:/home/lelong/job_script",
#    do_xcom_push=True,
#    dag=dag,
#)


send_script = PythonOperator(
    task_id='send_script',
    python_callable=log_scp_command,
    dag=dag
)


# Define the task to test SSH connection

test_ssh_connection = SSHOperator(
    task_id='test_ssh_connection',
    ssh_hook=ssh_hook,
    command='echo "SSH connection successful!"',
    do_xcom_push=True,
    dag=dag,
)


# Define the task to submit a SLURM job

submit_slurm_job = SSHOperator(
    task_id='submit_slurm_job',
    ssh_hook=ssh_hook,
    #ssh_conn_id='slurm_ssh_connection',
    command='sbatch /home/liuyuey/job_script/slurm_mlflow_script.slurm',
    dag=dag,
)


# # Task to submit the SLURM job
# submit_job = SSHOperator(
#     task_id='submit_job',
#     ssh_hook=ssh_hook,
#     command='sbatch /home/lelong/job_script/slurm_job.sh',
#     #do_xcom_push=True,
#     dag=dag,
# )


# Task to retrieve the output

retrieve_output = SSHOperator(
    task_id='retrieve_output',
    ssh_hook=ssh_hook,
    command='cat /home/liuyuey/job_output_test.txt 2>&1',
    do_xcom_push=True,
    dag=dag,
)


# Set task dependencies
