import sys
import logging
sys.path.insert(0, '/opt/airflow/dags/repo/plugins')
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from slurm_job_handler_new import SlurmJobHandlingSensor

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
remote_path='/home/lelong/log_airflow_slurm/scripts/'
local_path='/home/airflow/slurm_scripts/' 

# DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2024, 11, 1),
}


@dag(dag_id='sdc_sentinel_batch_update_daily_loop',
     default_args=default_args,
     description='Daily Update Sentinel-2 Imagery Loop on SDC',
     schedule_interval=None,
     start_date=days_ago(1),
     tags=['sdc', 'sentinel'])
def daily_sentinel_batch_processing_dag():

    def get_dates():
        return [#"cemsre_t55hbd_20241203",
                #"cemsre_t55jbf_20241203",
                #"cemsre_t55jbg_20241203",
                #"cemsre_t55jbh_20241203",
                #"cemsre_t55jbj_20241203",
                #"cemsre_t55jbk_20241203",
                #"cemsre_t55jcg_20241203",
                #"cemsre_t55jcj_20241203",
                #"cemsre_t55jck_20241203",
                "cemsre_t54jul_20241209",
                "cemsre_t54jup_20241209",
                "cemsre_t54juq_20241209",
                "cemsre_t54jvq_20241209"
               ]  # Assuming these dates are dynamically determined elsewhere

    get_list = PythonOperator(task_id="get_img_list",
                              python_callable=get_dates,
                              do_xcom_push=True)

    # Combine all commands into one large script
    for i, date in enumerate(get_dates()):
        # Task 1: Cloud fmask processing
        cloud_fmask_processing = SlurmJobHandlingSensor(
            task_id=f'sentt_{date}_s1',
            ssh_conn_id='slurm_ssh_connection',
            script_name=f'sentt_{date}_s1',
            remote_path=remote_path,
            local_path=local_path, 
            #stage_script=script_stage_1,
            #dag=dag,
            timeout=3600,
            poke_interval=30,
            date = date,
            stage = "1"
        )

        # Task 2: Topo masks processing
        topo_masks_processing = SlurmJobHandlingSensor(
            task_id=f'sentt_{date}_s2',
            ssh_conn_id='slurm_ssh_connection',
            script_name=f'sentt_{date}_s2',
            remote_path=remote_path,
            local_path=local_path, 
            #stage_script=script_stage_1,
            #dag=dag,
            timeout=3600,
            poke_interval=30,
            date = date,
            stage = "2",       
        )


        # Task 3: Surface reflectance processing
        surface_reflectance_processing = SlurmJobHandlingSensor(
            task_id=f'sentt_{date}_s3',
            ssh_conn_id='slurm_ssh_connection',
            script_name=f'sentt_{date}_s3',
            remote_path=remote_path,
            local_path=local_path, 
            #stage_script=script_stage_1,
            #dag=dag,
            timeout=3600,
            poke_interval=30,
            date = date,
            stage = "3",      
        )

        # Task 4: Water index processing
        water_index_processing = SlurmJobHandlingSensor(
            task_id=f'sentt_{date}_s4',
            ssh_conn_id='slurm_ssh_connection',
            script_name=f'sentt_{date}_s4',
            remote_path=remote_path,
            local_path=local_path, 
            #stage_script=script_stage_1,
            #dag=dag,
            timeout=3600,
            poke_interval=30,
            date = date,
            stage = "4",      
        )

        # Task 5: Fractional cover processing
        fractional_cover_processing = SlurmJobHandlingSensor(
            task_id=f'sentt_{date}_s5',
            ssh_conn_id='slurm_ssh_connection',
            script_name=f'sentt_{date}_s5',
            remote_path=remote_path,
            local_path=local_path, 
            #stage_script=script_stage_1,
            #dag=dag,
            timeout=3600,
            poke_interval=30,
            date = date,
            stage = "5",      
        )

        # Task Dependency Setup
        get_list >> cloud_fmask_processing >> topo_masks_processing >> surface_reflectance_processing >> water_index_processing >> fractional_cover_processing

dag_instance = daily_sentinel_batch_processing_dag()
