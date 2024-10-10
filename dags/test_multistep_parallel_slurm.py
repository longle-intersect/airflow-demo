# example_dag.py
import sys
sys.path.insert(0, '/opt/airflow/dags/repo/plugins')
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from slurm_job_handler import SlurmJobHandlingSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
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
sleep {kwargs['sec']}
echo "{kwargs['out_text']}"
"""
    script_path = f'/home/airflow/slurm_scripts/{kwargs['script_name']}'
    with open(script_path, 'w') as file:
        file.write(script_content)
    return script_path


with DAG('test_slurm_multistep_parrallel',
         default_args=default_args,
         description='A multi-step parallel DAG to submit and monitor Slurm jobs via SSH',
         schedule_interval=None,
         start_date=datetime(2023, 1, 1),
         catchup=False) as dag:

    # Define all tasks
    create_script_1 = PythonOperator(
        task_id='create_slurm_script_1',
        python_callable=create_slurm_script,
        op_kwargs={'script_name': f'{dag.dag_id}_1.slurm', 'sec': '30', 'out_text': "Finish task 1"},
        dag=dag,
    )

    handle_slurm_job_1 = SlurmJobHandlingSensor(
        task_id='handle_slurm_job_1',
        ssh_conn_id='slurm_ssh_connection',
        script_name=f'{dag.dag_id}_1.slurm',
        remote_path='/home/lelong/job_script',
        local_path='/home/airflow/slurm_scripts',
        dag=dag,
        timeout=3600,
        poke_interval=30
    )

    create_script_2 = PythonOperator(
        task_id='create_slurm_script_2',
        python_callable=create_slurm_script,
        op_kwargs={'script_name': f'{dag.dag_id}_2.slurm', 'sec': '15', 'out_text': "Finish task 2"},
        dag=dag,
    )

    handle_slurm_job_2 = SlurmJobHandlingSensor(
        task_id='handle_slurm_job_2',
        ssh_conn_id='slurm_ssh_connection',
        script_name=f'{dag.dag_id}_2.slurm',
        remote_path='/home/lelong/job_script',
        local_path='/home/airflow/slurm_scripts',
        dag=dag,
        timeout=3600,
        poke_interval=30
    )

    create_script_3 = PythonOperator(
        task_id='create_slurm_script_3',
        python_callable=create_slurm_script,
        op_kwargs={'script_name': f'{dag.dag_id}_3.slurm', 'sec': '40', 'out_text': "Finish task 3"},
        dag=dag,
    )

    handle_slurm_job_3 = SlurmJobHandlingSensor(
        task_id='handle_slurm_job_3',
        ssh_conn_id='slurm_ssh_connection',
        script_name=f'{dag.dag_id}_3.slurm',
        remote_path='/home/lelong/job_script',
        local_path='/home/airflow/slurm_scripts',
        dag=dag,
        timeout=3600,
        poke_interval=30
    )

    create_script_4 = PythonOperator(
        task_id='create_slurm_script_4',
        python_callable=create_slurm_script,
        op_kwargs={'script_name': f'{dag.dag_id}_4.slurm', 'sec': '20', 'out_text': "Finish task 4"},
        dag=dag,
    )

    handle_slurm_job_4 = SlurmJobHandlingSensor(
        task_id='handle_slurm_job_4',
        ssh_conn_id='slurm_ssh_connection',
        script_name=f'{dag.dag_id}_4.slurm',
        remote_path='/home/lelong/job_script',
        local_path='/home/airflow/slurm_scripts',
        dag=dag,
        timeout=3600,
        poke_interval=30
    )

    # Set task dependencies
    create_script_1 >> handle_slurm_job_1
    handle_slurm_job_1 >> [create_script_2, create_script_3]
    create_script_2 >> handle_slurm_job_2
    create_script_3 >> handle_slurm_job_3
    [handle_slurm_job_2, handle_slurm_job_3] >> create_script_4
    create_script_4 >> handle_slurm_job_4