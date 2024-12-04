import sys
import logging
sys.path.insert(0, '/opt/airflow/dags/repo/plugins')
import re
from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from slurm_job_handler_new import SlurmJobHandlingSensor
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain
from airflow.models import XCom
import base64

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
remote_path='/home/lelong/log_airflow_slurm/scripts/'
local_path='/home/airflow/slurm_scripts/' 

# Function to parse the output and extract file names

# Function to extract the tile identifier and date from filenames
# def parse_file_list(ti):
#     file_list = ti.xcom_pull(task_ids='download_files')
#     decoded_list = base64.b64decode(file_list).decode()
#     print(decoded_list)
#     pattern = re.compile(r"T\d{2}[A-Z]{3}_\d{8}")
#     processed_list = [pattern.search(filename).group(0).lower() for filename in eval(decoded_list)]
#     processed_list = ["cemsre_" + filename for filename in processed_list]

#     return processed_list


# DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2024, 12, 1),
}


with DAG(dag_id='sdc_sentinel_batch_ingest_update_daily_2',
     default_args=default_args,
     description='Daily Ingest and Update Sentinel-2 Imagery using TaskGroup 2 on SDC',
     schedule_interval=None,
     start_date=days_ago(1),
     tags=['sdc', 'sentinel']) as dag:

    #dates = ["20241018", "20241011", "20241008", "20241001"]  # Assuming these dates are dynamically determined elsewhere

    # get_list = PythonOperator(task_id="get_img_list",
    #                           python_callable=get_dates,
    #                           do_xcom_push=True)

    # SSH to list files in the directory
    # ls *.img *.meta >> newer.txt;
    # cat newer.txt;
    download_files = SSHOperator(
        task_id='download_files',
        ssh_conn_id='slurm_ssh_connection',
        command="""
        module load sdc_testing;
        cd $FILESTORE_PATH/download/;
        python ~/workspace/updateSentinel_fromSara.py --sentinel 2 --regionofinterest $RSC_SENTINEL2_DFLT_REGIONOFINTEREST --startdate 2024-12-03 --numdownloadthreads 4  --logdownloadspeed --saraparam "processingLevel=L1C";
        """,
        conn_timeout=3600,
        cmd_timeout=3600,
        do_xcom_push=True  # Pushes the command output to XCom
    )

    @task
    def parse_file_list(file_list):
        #file_list = ti.xcom_pull(task_ids='download_files')
        decoded_list = base64.b64decode(file_list).decode()
        print(decoded_list)
        pattern = re.compile(r"T\d{2}[A-Z]{3}_\d{8}")
        processed_list = [pattern.search(filename).group(0).lower() for filename in eval(decoded_list)]
        processed_list = ["cemsre_" + filename for filename in processed_list]

        return processed_list
    
    get_new_list = parse_file_list(download_files.output)

    def create_processing_tasks(date):

        @task_group(group_id=f'process_{date}')
        def process_image(date):
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
            cloud_fmask_processing >> topo_masks_processing >> surface_reflectance_processing >> water_index_processing >> fractional_cover_processing
        
        process_image(date)

    @task_group
    def process_files_task_group(dates):
        create_processing_tasks(dates)
    # expand method is used for the creation of dynamic tasks
    process_files_task_group.expand(dates=get_new_list())

