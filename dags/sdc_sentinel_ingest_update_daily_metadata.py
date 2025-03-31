import sys
import logging
sys.path.insert(0, '/opt/airflow/dags/repo/plugins')
sys.path.insert(0, '/opt/airflow/')
import re
from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from plugins.slurm_job_handler_new import SlurmJobHandlingSensorSentinel
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain
from airflow.models import XCom, Variable
import base64

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
remote_path='/home/lelong/log_airflow_slurm/scripts/'
local_path='/opt/airflow/slurm_scripts/' 

# Function to parse the output and extract file names

# Function to extract the tile identifier and date from filenames
def parse_file_list(ti):
    file_list = ti.xcom_pull(task_ids='download_files')
    decoded_list = base64.b64decode(file_list).decode()
    print(decoded_list)
    pattern = re.compile(r"T\d{2}[A-Z]{3}_\d{8}")
    processed_list = []
    for filename in eval(decoded_list):
        # Extract the tile identifier and date
        match = pattern.search(filename)
        if match:
            extracted = match.group(0).lower()
            # Prepend based on the prefix
            if filename.startswith("S2A"):
                processed_list.append("cemsre_" + extracted)
            elif filename.startswith("S2B"):
                processed_list.append("cfmsre_" + extracted)
            elif filename.startswith("S2C"):
                processed_list.append("cgmsre_" + extracted)
            else:
                processed_list.append(extracted)

    print(processed_list)
    Variable.set("new_list", processed_list, serialize_json=True)
    return processed_list


# DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2024, 12, 9),
}


@dag(dag_id='sdc_sentinel_batch_ingest_metadata_update_daily',
     default_args=default_args,
     description='Daily Ingest, Processing and Update Sentinel-2 Imagery using TaskGroup on SDC',
     schedule_interval=None,
     start_date=days_ago(1),
     tags=['sdc', 'sentinel', 'metadata'])
def daily_sentinel_batch_ingest_processing_dag():
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
        python ~/workspace/updateSentinel_fromSara.py --sentinel 2 --regionofinterest $RSC_SENTINEL2_DFLT_REGIONOFINTEREST --startdate 2024-12-09 --numdownloadthreads 4  --logdownloadspeed --saraparam "processingLevel=L1C";
        """,
        conn_timeout=3600,
        cmd_timeout=3600,
        do_xcom_push=True  # Pushes the command output to XCom
    )

    get_new_list = PythonOperator(
        task_id="get_new_list",
        python_callable=parse_file_list,
    )

    @task_group(group_id='batch_processing')
    def process_date_group():
        mapped_args = Variable.get("new_list", default_var=[], deserialize_json=True)

        # Expand the dynamic task group only if mapped_args is not empty
        if mapped_args:
            for i, date in enumerate(mapped_args):
                @task_group(group_id=f"process_img_{i}")
                def dynamic_task_group_node(date):
                    # Create a custom task for each mapped argument
                    
                    # Task 1: Cloud fmask processing
                    cloud_fmask_processing = SlurmJobHandlingSensorSentinel(
                        task_id=f'i{i}_s1',
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
                    topo_masks_processing = SlurmJobHandlingSensorSentinel(
                        task_id=f'i{i}_s2',
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
                    surface_reflectance_processing = SlurmJobHandlingSensorSentinel(
                        task_id=f'i{i}_s3',
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
                    water_index_processing = SlurmJobHandlingSensorSentinel(
                        task_id=f'i{i}_s4',
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
                    fractional_cover_processing = SlurmJobHandlingSensorSentinel(
                        task_id=f'i{i}_s5',
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

                dynamic_task_group_node(date)

    # Link the start task to the task group
    download_files >> get_new_list >> process_date_group()

dag_instance = daily_sentinel_batch_ingest_processing_dag()


