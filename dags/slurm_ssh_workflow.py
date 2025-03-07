# example_dag.py
import sys
import logging
sys.path.insert(0, '/opt/airflow/dags/repo/plugins')
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from slurm_ssh_operator import SlurmSSHTaskOperator, SlurmJobSensor
from airflow.operators.python import PythonOperator
# slurm_ssh_operator.py

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

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
    script_path = '/opt/airflow/slurm_scripts/test_slurm_ssh.slurm'
    with open(script_path, 'w') as file:
        file.write(script_content)
    return script_path

with DAG('slurm_ssh_workflow',
         default_args=default_args,
         description='A simple DAG to submit and monitor Slurm jobs via SSH',
         schedule_interval=None,
         start_date=datetime(2023, 1, 1),
         catchup=False) as dag:

    #start = DummyOperator(task_id='start')

    # Get the user-specific SSH connection ID from the provided configuration
    def get_user_ssh_conn_id(**kwargs):
        user = kwargs['dag_run'].conf.get('user', 'admin')
        #logging.INFO(f"Current User is {user}")
        return f'slurm_ssh_{user}'

    get_user_ssh_conn_id = PythonOperator(
        task_id='get_user_ssh_conn_id',
        python_callable=get_user_ssh_conn_id,
        provide_context=True,
        dag=dag,
    )

    create_script = PythonOperator(
        task_id='create_slurm_script',
        python_callable=create_slurm_script,
        provide_context=True,
        dag=dag,
    )

    submit_slurm_job = SlurmSSHTaskOperator(
        task_id='submit_slurm_job',
        #ssh_conn_id='slurm_ssh_connection',
        ssh_conn_id="{{ task_instance.xcom_pull(task_ids='get_user_ssh_conn_id') }}",
        script_name='test_slurm_ssh.slurm',
        remote_path='/home/lelong/job_script'
    )

    monitor_slurm_job = SlurmJobSensor(
        task_id='monitor_slurm_job',
        #ssh_conn_id='slurm_ssh_connection',
        ssh_conn_id="{{ task_instance.xcom_pull(task_ids='get_user_ssh_conn_id') }}",
        task_ids = 'submit_slurm_job',
        remote_path='/home/lelong/job_script',
        job_id="{{ task_instance.xcom_pull(task_ids='submit_slurm_job') }}"
    )


    #end = DummyOperator(task_id='end')

    #start >> create_script >> submit_slurm_job >> monitor_slurm_job >> end

    create_script >> get_user_ssh_conn_id >> submit_slurm_job >> monitor_slurm_job
