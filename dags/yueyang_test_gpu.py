# example_dag.py
import os
import logging
import sys
sys.path.insert(0, '/opt/airflow/dags/repo/plugins')
sys.path.insert(0, '/opt/airflow/')

from airflow import DAG
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.exceptions import AirflowException
from plugins.slurm_job_handler_gpu import SlurmJobHandlingSensor
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.models import XCom, Variable


# Set up logging
logger = logging.getLogger("airflow.task")
remote_path='/home/liuyuey/slurm_script/'
local_path='/opt/airflow/slurm_script/' 


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Python function to create SLURM script
def create_slurm_script(**kwargs):
    script_content = f"""#!/bin/bash
#SBATCH --job-name=test_airflow_gpu_job
#SBATCH --output={remote_path}/test_job_output.txt
#SBATCH --error={remote_path}/test_job_error.txt
#SBATCH -n 1
#SBATCH --mem=500M
#SBATCH -t 00:10:00
#SBATCH --partition=GPU
#SBATCH --gres=gpu:1   # allowed number range: 1-2
#SBATCH --nodelist=sdccomp07   # available nodes: sdccomp06,07,13

# Specify the work to be done
sleep {kwargs['sec']}
echo "{kwargs['out_text']}"
nvidia-smi
"""
    script_path = f'{local_path}/{kwargs['script_name']}'
    with open(script_path, 'w') as file:
        file.write(script_content)
    return script_path


with DAG('yueyang_test_slurm_gpu',
         default_args=default_args,
         description='A multi-step DAG to submit and monitor Slurm jobs via SSH',
         schedule_interval=None,
         start_date=datetime(2023, 1, 1),
         catchup=False) as dag:

    create_script_1 = PythonOperator(
        task_id='create_slurm_script_1',
        python_callable=create_slurm_script,
        op_kwargs={'script_name': 'test_script_1.slurm',
                    'sec': '30',
                    'out_text': "Finish task 1"},
        provide_context=True,
        dag=dag,
    )

    handle_slurm_job_1 = SlurmJobHandlingSensor(
        task_id='handle_slurm_job_1',
        ssh_conn_id='slurm_ssh_connection_yueyang',
        script_name='test_script_1.slurm',
        remote_path=remote_path,
        local_path=local_path,
        dag=dag,
        timeout=3600,  # max time in seconds for the sensor to run
        poke_interval=30  # interval in seconds between checks
    )

    create_script_1 >> handle_slurm_job_1 
