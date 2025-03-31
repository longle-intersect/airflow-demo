import sys
import logging
#sys.path.insert(0, '/opt/airflow/dags/repo/plugins')
sys.path.insert(0, '/opt/airflow/')
import re
from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from plugins.slurm_job_handler_new import SlurmJobHandlingSensorSentinel
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain
from airflow.models import XCom, Variable
import base64

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
remote_path='/home/lelong/log_airflow_slurm/scripts/'
local_path='/home/airflow/slurm_scripts/' 

# Function to parse the output and extract file names

# Function to extract the tile identifier and date from filenames

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


@dag(dag_id='sdc_sentinel_metadata_parsing',
     default_args=default_args,
     description='Parsing Metadata for Sentinel-2 Imagery on SDC',
     schedule_interval=None,
     start_date=days_ago(1),
     tags=['sdc', 'sentinel'])
def daily_sentinel_metadata_parsing_dag():
    # get_list = PythonOperator(task_id="get_img_list",
    #                           python_callable=get_dates,
    #                           do_xcom_push=True)

    # SSH to list files in the directory
    # ls *.img *.meta >> newer.txt;
    # cat newer.txt;
    start = DummyOperator(task_id="start")

    parsing_metadata = SSHOperator(
        task_id='parsing_metadata',
        ssh_conn_id='slurm_ssh_connection',
        command="""
        module load sdc_testing;
        cd $FILESTORE_PATH/download/;
        python ~/workspace/history2json.py cgmsre_t55jbj_20250211_ad5m5.img;
        cp output_metadata.json ~/workspace/cgmsre_t55jbj_20250211.json
        """,
        conn_timeout=3600,
        cmd_timeout=3600,
        do_xcom_push=True  # Pushes the command output to XCom
    )

    end = DummyOperator(task_id="end")

    # Link the start task to the task group
    start >> parsing_metadata >> end

dag_instance = daily_sentinel_metadata_parsing_dag()


