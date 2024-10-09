# example_dag.py
import sys
sys.path.insert(0, '/opt/airflow/dags/repo/plugins')
from airflow import DAG
from slurm_job_handler import SlurmJobHandlingSensor
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Python function to create SLURM script
def create_slurm_script(**kwargs):
    script_content = """#!/bin/bash
#SBATCH --job-name=test_airflow_job
#SBATCH --output=./job_script/test_job_output.txt
#SBATCH --error=./job_script/test_job_error.txt
#SBATCH -n 1
#SBATCH --mem=500M
#SBATCH -t 00:10:00

# Specify the work to be done
sleep 30
echo "Welcome to SDC! I'm Long Le"
"""
    script_path = '/home/airflow/slurm_scripts/test_slurm_ssh.slurm'
    with open(script_path, 'w') as file:
        file.write(script_content)
    return script_path


with DAG('test_slurm_single_step',
         default_args=default_args,
         description='A single step DAG to submit and monitor Slurm jobs via SSH',
         schedule_interval=None,
         start_date=datetime(2024, 10, 10),
         catchup=False) as dag:

    create_script = PythonOperator(
        task_id='create_slurm_script',
        python_callable=create_slurm_script,
        provide_context=True,
        dag=dag,
    )

    handle_slurm_job = SlurmJobHandlingSensor(
        task_id='handle_slurm_job',
        ssh_conn_id='slurm_ssh_connection',
        script_name='test_slurm_ssh.slurm',
        remote_path='/home/lelong/job_script',
        local_path='/home/airflow/slurm_scripts/',
        dag=dag,
        timeout=3600,  # max time in seconds for the sensor to run
        poke_interval=30  # interval in seconds between checks
    )

    create_script >> handle_slurm_job
