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
dates = ["cemsre_t55hbd_20241203",
        "cemsre_t55jbf_20241203",
        "cemsre_t55jbg_20241203",
        "cemsre_t55jbh_20241203",
        "cemsre_t55jbj_20241203",
        "cemsre_t55jbk_20241203",
        "cemsre_t55jcg_20241203",
        "cemsre_t55jcj_20241203",
        "cemsre_t55jck_20241203"
]  # Assuming these dates are dynamically determined elsewhere
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


@dag(dag_id='sdc_sentinel_batch_ingest_update_daily_2',
     default_args=default_args,
     description='Daily Ingest and Update Sentinel-2 Imagery using TaskGroup 2 on SDC',
     schedule_interval=None,
     start_date=days_ago(1),
     tags=['sdc', 'sentinel'])
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
        python ~/workspace/updateSentinel_fromSara.py --sentinel 2 --regionofinterest $RSC_SENTINEL2_DFLT_REGIONOFINTEREST --startdate 2024-12-03 --numdownloadthreads 4  --logdownloadspeed --saraparam "processingLevel=L1C";
        """,
        conn_timeout=3600,
        cmd_timeout=3600,
        do_xcom_push=True  # Pushes the command output to XCom
    )

    @task
    def parse_file_list(xcom_arg):
        file_list_str = base64.b64decode(xcom_arg).decode('utf-8')
        pattern = re.compile(r"T\d{2}[A-Z]{3}_\d{8}")
        processed_list = [pattern.search(filename).group(0).lower() for filename in eval(file_list_str) if pattern.search(filename)]
        return ["cemsre_" + filename for filename in processed_list]

    parsed_files = parse_file_list(download_files.output)

    
    @task_group(group_id='image_processing')
    def process_date_group(date):
        # Dummy implementation of task groups
        print(f"Processing date: {date}")

    for date in parsed_files:
        process_date_group(date)

dag_instance = daily_sentinel_batch_ingest_processing_dag()


