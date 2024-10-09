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
    script_content = f"""#!/bin/bash
#SBATCH --job-name=test_airflow_job
#SBATCH --output=./job_script/test_job_output.txt
#SBATCH --error=./job_script/test_job_error.txt
#SBATCH -n 1
#SBATCH --mem=500M
#SBATCH -t 00:10:00

# Specify the work to be done
sleep {sec}
echo "Welcome to SDC! I'm Long Le"
"""
    script_path = f'/home/airflow/slurm_scripts/{script_name}'
    with open(script_path, 'w') as file:
        file.write(script_content)
    return script_path


with DAG('test_slurm_multi_step',
         default_args=default_args,
         description='A multi-step DAG to submit and monitor Slurm jobs via SSH',
         schedule_interval=None,
         start_date=datetime(2023, 1, 1),
         catchup=False) as dag:

    create_script_1 = PythonOperator(
        task_id='create_slurm_script_1',
        python_callable=create_slurm_script,
        op_kwargs={'script_name': 'test_script_1.slurm', 'sec': '30'},
        provide_context=True,
        dag=dag,
    )

    handle_slurm_job_1 = SlurmJobHandlingSensor(
        task_id='handle_slurm_job_1',
        ssh_conn_id='slurm_ssh_connection',
        script_name='test_script_1.slurm',
        remote_path='/home/lelong/job_script',
        local_path='/home/airflow/slurm_scripts/',
        dag=dag,
        timeout=3600,  # max time in seconds for the sensor to run
        poke_interval=30  # interval in seconds between checks
    )

    create_script_2 = PythonOperator(
        task_id='create_slurm_script_2',
        python_callable=create_slurm_script,
        op_kwargs={'script_name': 'test_script_2.slurm', 'sec': '15'},
        provide_context=True,
        dag=dag,
    )

    handle_slurm_job_2 = SlurmJobHandlingSensor(
        task_id='handle_slurm_job_2',
        ssh_conn_id='slurm_ssh_connection',
        script_name='test_script_2.slurm',
        remote_path='/home/lelong/job_script',
        local_path='/home/airflow/slurm_scripts/',
        dag=dag,
        timeout=3600,  # max time in seconds for the sensor to run
        poke_interval=30  # interval in seconds between checks
    )

    create_script_3 = PythonOperator(
        task_id='create_slurm_script_3',
        python_callable=create_slurm_script,
        op_kwargs={'script_name': 'test_script_3.slurm', 'sec': '40'},
        provide_context=True,
        dag=dag,
    )

    handle_slurm_job_3 = SlurmJobHandlingSensor(
        task_id='handle_slurm_job_3',
        ssh_conn_id='slurm_ssh_connection',
        script_name='test_script_3.slurm',
        remote_path='/home/lelong/job_script',
        local_path='/home/airflow/slurm_scripts/',
        dag=dag,
        timeout=3600,  # max time in seconds for the sensor to run
        poke_interval=30  # interval in seconds between checks
    )

    create_script_4 = PythonOperator(
        task_id='create_slurm_script_4',
        python_callable=create_slurm_script,
        op_kwargs={'script_name': 'test_script_4.slurm', 'sec': '20'},
        provide_context=True,
        dag=dag,
    )

    handle_slurm_job_4 = SlurmJobHandlingSensor(
        task_id='handle_slurm_job_4',
        ssh_conn_id='slurm_ssh_connection',
        script_name='test_script_4.slurm',
        remote_path='/home/lelong/job_script',
        local_path='/home/airflow/slurm_scripts/',
        dag=dag,
        timeout=3600,  # max time in seconds for the sensor to run
        poke_interval=30  # interval in seconds between checks
    )

    create_script_1 >> handle_slurm_job_1 >> create_script_2 >> handle_slurm_job_2 >> create_script_3 >> handle_slurm_job_3 >> create_script_4 >> handle_slurm_job_4
