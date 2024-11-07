import sys
sys.path.insert(0, '/opt/airflow/dags/repo/plugins')
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from slurm_job_handler_new import SlurmJobHandlingSensor

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
remote_path='/home/lelong/log_airflow_slurm/scripts/',
local_path='/home/airflow/slurm_scripts/', 

# DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}


with DAG(
    'sdc_sentinel_update_single',
    default_args=default_args,
    description='Processing Single Sentinel Images on SDC',
    schedule_interval=None,  # Set to None for manual triggering
    start_date=days_ago(1),
    tags=['sdc', 'sentinel', "single"],
) as dag:

    date = "20241008"
    script_name = f'sentt_{date}'
    # Define all tasks
    # script_stage_1_path = create_slurm_script(**{'script_name': f'{dag.dag_id}_s1.slurm',
    #                                           'stage': '1',
    #                                           'stage_script': f'{script_stage_1}'})

    # Task 1: Cloud fmask processing
    cloud_fmask_processing = SlurmJobHandlingSensor(
        task_id=f'{script_name}_s1',
        ssh_conn_id='slurm_ssh_connection',
        script_name=f'{script_name}_s1',
        remote_path=remote_path,
        local_path=local_path, 
        #stage_script=script_stage_1,
        dag=dag,
        timeout=3600,
        poke_interval=30,
        date = date,
        stage = "1"
    )

    # Define all tasks
    # script_stage_2_path = create_slurm_script(**{'script_name': f'{dag.dag_id}_s2.slurm',
    #                                           'stage': '2',
    #                                           'stage_script': f'{script_stage_2}'})

    # Task 2: Topo masks processing
    topo_masks_processing = SlurmJobHandlingSensor(
        task_id=f'{script_name}_s2',
        ssh_conn_id='slurm_ssh_connection',
        script_name=f'{script_name}_s2',
        remote_path=remote_path,
        local_path=local_path, 
        #stage_script=script_stage_2,
        dag=dag,
        timeout=3600,
        poke_interval=30,
        date = date,
        stage = "2"
    )

    # Define all tasks
    # script_stage_3_path = create_slurm_script(**{'script_name': f'{dag.dag_id}_s3.slurm',
    #                                           'stage': '3',
    #                                           'stage_script': f'{script_stage_3}'})
    # Task 3: Surface reflectance processing
    surface_reflectance_processing = SlurmJobHandlingSensor(
        task_id=f'{script_name}_s3',
        ssh_conn_id='slurm_ssh_connection',
        script_name=f'{script_name}_s3',
        remote_path=remote_path,
        local_path=local_path, 
        #stage_script=script_stage_3,        
        dag=dag,
        timeout=3600,
        poke_interval=30,
        date = date,
        stage = "3"     
    )

    # Define all tasks
    # script_stage_4_path = create_slurm_script(**{'script_name': f'{dag.dag_id}_s4.slurm',
    #                                           'stage': '4',
    #                                           'stage_script': f'{script_stage_4}'})
    # Task 4: Water index processing
    water_index_processing = SlurmJobHandlingSensor(
        task_id=f'{script_name}_s4',
        ssh_conn_id='slurm_ssh_connection',
        script_name=f'{script_name}_s4',
        remote_path=remote_path,
        local_path=local_path,
        #stage_script=script_stage_4,         
        dag=dag,
        timeout=3600,
        poke_interval=30,
        date = date,
        stage = "4"       
    )

    # Define all tasks
    # script_stage_5_path = create_slurm_script(**{'script_name': f'{dag.dag_id}_s5.slurm',
    #                                           'stage': '5',
    #                                           'stage_script': f'{script_stage_5}'})
    # Task 8: Fractional cover processing
    fractional_cover_processing = SlurmJobHandlingSensor(
        task_id=f'{script_name}_s5',
        ssh_conn_id='slurm_ssh_connection',
        script_name=f'{script_name}_s5',
        remote_path=remote_path,
        local_path=local_path,
        #stage_script=script_stage_5,        
        dag=dag,
        timeout=3600,
        poke_interval=30,
        date = date,
        stage = "5"           
    )

    # Task Dependency Setup
    cloud_fmask_processing >> topo_masks_processing >> surface_reflectance_processing >> water_index_processing >> fractional_cover_processing

