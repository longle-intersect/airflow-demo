import sys
import logging
import os
import ast
sys.path.insert(0, '/opt/airflow/dags/repo/plugins')
sys.path.insert(0, '/opt/airflow/')
import re
from datetime import datetime, timedelta
import subprocess
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional
import time

from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from plugins.slurm_job_handler_new import SlurmJobHandlingSensorSentinel
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain
from airflow.models import XCom, Variable


# Set up logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger("airflow.task")
# remote_path='/home/lelong/log_airflow_slurm/scripts/'
# local_path='/opt/airflow/slurm_script/' 
# local_tmp_path='/opt/airflow/tmp_files/'
# local_metadata_path ='/opt/airflow/tmp_metadata/'
# shared_dir = "/mnt/scratch_lustre/tmp/rs_testing/tmp_shared/"  # Shared filestore between HPC and AARNet
# MAX_URLS = 10  # Assume a maximum of 10 URLs

remote_path = Variable.get("remote_path")
local_path  = Variable.get("local_path") 
local_tmp_path = Variable.get("local_tmp_path") 
local_metadata_path = Variable.get("local_metadata_path") 
shared_dir = Variable.get("shared_dir")
local_tmp_path = Variable.get("local_tmp_path")
MAX_JOBS = int(Variable.get("MAX_JOBS"))
# Function to parse the output and extract file names

# Function to extract the tile identifier and date from filenames
# def parse_file_list(ti):
#     file_list = ti.xcom_pull(task_ids='download_files')
#     decoded_list = base64.b64decode(file_list).decode()
#     print(decoded_list)
#     pattern = re.compile(r"T\d{2}[A-Z]{3}_\d{8}")
#     processed_list = []
#     for filename in eval(decoded_list):
#         # Extract the tile identifier and date
#         match = pattern.search(filename)
#         if match:
#             extracted = match.group(0).lower()
#             # Prepend based on the prefix
#             if filename.startswith("S2A"):
#                 processed_list.append("cemsre_" + extracted)
#             elif filename.startswith("S2B"):
#                 processed_list.append("cfmsre_" + extracted)
#             elif filename.startswith("S2C"):
#                 processed_list.append("cgmsre_" + extracted)
#             else:
#                 processed_list.append(extracted)

#     print(processed_list)
#     Variable.set("new_list", processed_list, serialize_json=True)
#     return processed_list


#@task
def search_new_images():
    """
    SSH into the remote server to search for new images.
    Executes the command, parses output for XCOM values,
    downloads a file via SFTP, and returns a dictionary.
    """
    try:
        logger.info("Starting SSH task execution for search")
        ssh_hook = SSHHook(ssh_conn_id='slurm_ssh_connection')
        ssh_client = ssh_hook.get_conn()

        # Build the command
        command = (
            f'cd {shared_dir} && '
            'python ~/workspace/updateSentinel_fromSara_new.py --task search --sentinel 2 '
            '--regionofinterest $RSC_SENTINEL2_DFLT_REGIONOFINTEREST --startdate 2025-03-21 '
            '--numdownloadthreads 4 --logdownloadspeed --saraparam "processingLevel=L1C"'
        )
        logger.info(f"Executing command: {command}")

        stdin, stdout, stderr = ssh_client.exec_command(command, timeout=300)
        exit_status = stdout.channel.recv_exit_status()
        output = stdout.read().decode('utf-8')
        error_output = stderr.read().decode('utf-8')

        logger.info(f"Command exit status: {exit_status}")
        logger.info(f"Command output: {output}")
        if error_output:
            logger.error(f"Command stderr: {error_output}")

        if exit_status != 0:
            raise AirflowException(f"SSH command failed with exit code {exit_status}. Error: {error_output}")

        # Parse the output for XCOM values
        url_list = None
        url_file = None
        for line in output.split('\n'):
            if line.startswith('XCOM_URL_LIST:'):
                url_list = line.replace('XCOM_URL_LIST:', '').strip()
            elif line.startswith('XCOM_URL_FILE:'):
                url_file = line.replace('XCOM_URL_FILE:', '').strip()

        if not url_list or not url_file:
            raise AirflowException("Failed to retrieve URL list or file from command output.")

        # Download the file via SFTP
        file_list_name = os.path.basename(url_file)
        os.makedirs(local_tmp_path, mode=0o777, exist_ok=True)
        sftp_client = ssh_client.open_sftp()
        remote_filelist = os.path.join(shared_dir, file_list_name)
        local_filelist = os.path.join(local_tmp_path, file_list_name)
        sftp_client.get(remote_filelist, local_filelist)
        sftp_client.close()

        logger.info(f"Retrieved URL List: {url_list}")
        logger.info(f"Retrieved URL File: {url_file}")

        # Convert the URL list string to an actual Python list.
        urls = ast.literal_eval(url_list)
        #return {"urls": urls, "url_file": url_file}
        return urls

    except Exception as e:
        logger.error(f"Task failed with exception: {str(e)}", exc_info=True)
        raise AirflowException(f"Search task failed: {str(e)}")
    finally:
        if 'ssh_client' in locals():
            logger.info("Closing SSH connection for search")
            ssh_client.close()

#@task
def download_url(url: str):
    """
    Download the content from a given URL via SSH using a curl command.
    First it sends a HEAD request to resolve the final URI and then downloads the file.
    """
    logger.info(f"Starting download task for URL: {url}")
    aarnet_hook = SSHHook(ssh_conn_id='aarnet_ssh_connection')
    curl_cmd_filename = f"cd {shared_dir} && curl --silent -n --location --head {url}"
    curl_cmd = f"cd {shared_dir} && curl -n -L -O -J --silent --show-error {url}"

    ok = True
    # Get final URI using HEAD request
    try:
        ssh_client = aarnet_hook.get_conn()
        stdin, stdout, stderr = ssh_client.exec_command(curl_cmd_filename)


        exit_status = stdout.channel.recv_exit_status()
        stdout_output = stdout.read().decode('utf-8').strip()
        stderr_output = stderr.read().decode('utf-8').strip()
        logger.info(f"HEAD stdout: {stdout_output}")

        uri = None
        for line in stdout_output.split('\n'):
            if line.startswith('Location: '):
                uri = line.split(' ', 1)[1].strip()
                break

        zipfileName = uri.split('/')[-1]
        if exit_status != 0 or stderr_output or not uri:
            logger.error(f"Failed to resolve URI for {url}: {stderr_output}")
            ok = False
            return None
    except Exception as e:
        logger.error(f"Error during HEAD request for {url}: {str(e)}")
        ok = False
        return None
    finally:
        ssh_client.close()

    # Download file using the resolved URI
    try:
        ssh_client = aarnet_hook.get_conn()
        startTime = time.time()
        stdin, stdout, stderr = ssh_client.exec_command(curl_cmd)
        endTime = time.time()
        downloadTime = endTime - startTime

        exit_status = stdout.channel.recv_exit_status()
        stderr_output = stderr.read().decode().strip()
        if exit_status != 0 or stderr_output:
            logger.error(f"Failed to download {uri}: {stderr_output}")
            return None

        logger.info(f"Successfully downloaded {uri} to {shared_dir}")
        logger.info(f"Download Time: {downloadTime}")

        ## TODO: Check zipfile is ok

        return zipfileName
    
    except Exception as e:
        logger.error(f"Error during download for {url}: {str(e)}")
        return None
    finally:
        ssh_client.close()

def get_file_name(filename):
    
    pattern = re.compile(r"T\d{2}[A-Z]{3}_\d{8}")
    # Extract the tile identifier and date
    match = pattern.search(filename)
    qvf_name = None
    if match:
        extracted = match.group(0).lower()
        # Prepend based on the prefix
        if filename.startswith("S2A"):
            qvf_name = "cemsre_" + extracted
        elif filename.startswith("S2B"):
            qvf_name = "cfmsre_" + extracted
        elif filename.startswith("S2C"):
            qvf_name = "cgmsre_" + extracted

    return qvf_name

def import_file(zipfileName: str):
    """
    SSH into the remote server to search for new images.
    Executes the command, parses output for XCOM values,
    downloads a file via SFTP, and returns a dictionary.
    """
    try:
        logger.info(f"Starting import task for {zipfileName}")
        ssh_hook = SSHHook(ssh_conn_id='slurm_ssh_connection')
        ssh_client = ssh_hook.get_conn()

        # Build the command
        command = (
            f'module load sdc_testing && '
            f'cd {shared_dir} && '
            f'python ~/workspace/updateSentinel_fromSara_new.py --task import --zipfile {zipfileName} --shared_dir {shared_dir} '
            '--sentinel 2 --regionofinterest $RSC_SENTINEL2_DFLT_REGIONOFINTEREST --startdate 2025-03-21 '
            '--numdownloadthreads 4 --logdownloadspeed --saraparam "processingLevel=L1C"'
        )
        logger.info(f"Executing command: {command}")

        stdin, stdout, stderr = ssh_client.exec_command(command, timeout=300)
        exit_status = stdout.channel.recv_exit_status()
        output = stdout.read().decode('utf-8')
        error_output = stderr.read().decode('utf-8')

        logger.info(f"Command exit status: {exit_status}")
        logger.info(f"Command output: {output}")
        if error_output:
            logger.error(f"Command stderr: {error_output}")

        if exit_status != 0:
            raise AirflowException(f"SSH command failed with exit code {exit_status}. Error: {error_output}")

        # Parse the output for XCOM values
        qvf_name = get_file_name(zipfileName)
        logger.info(f"QVF Name: {qvf_name}")

        if not qvf_name:
            raise AirflowException("Failed to get qvf file name for the zip.")

        return qvf_name

    except Exception as e:
        logger.error(f"Task failed with exception: {str(e)}", exc_info=True)
        raise AirflowException(f"Search task failed: {str(e)}")
    finally:
        if 'ssh_client' in locals():
            logger.info("Closing SSH connection for search")
            ssh_client.close()

def test_download_url(url):
    logger.info(f"Downloading URL: {url}")
    return "S2B_MSIL1C_20250329T000219_N0511_R030_T56JLQ_20250329T005127.zip"

def test_import_file(zipfileName):
    logger.info(f"Importing file: {zipfileName}")
    return "cfmsre_56jlq"

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


@dag(dag_id='sdc_sentinel_batch_ingest_update_daily_AARNet_full',
     default_args=default_args,
     description='Daily Ingest with AARNet and Update Sentinel-2 Imagery using TaskGroup on SDC full processing',
     schedule_interval=None,
     start_date=days_ago(1),
     concurrency = 4,
     tags=['sdc', 'sentinel', 'daily'])
def daily_sentinel_batch_AARNet_processing_dag_1():

    search_files = PythonOperator(
        task_id='search_new_images',
        python_callable=search_new_images,
    )

    with TaskGroup("batch_processing") as tg:
        for i in range(MAX_JOBS):
            with TaskGroup(f"processing_{i}") as pg:
                download = PythonOperator(
                    task_id=f'download_{i}',
                    python_callable=download_url,
                    op_kwargs={
                        'url': f'{{{{ ti.xcom_pull(task_ids="search_new_images")[{i}] if ti.xcom_pull(task_ids="search_new_images") else None }}}}'
                    },
                    provide_context=True,  # Ensures context is passed for XCom access
                )

                import_files = PythonOperator(
                    task_id=f'import_{i}',
                    python_callable=import_file,
                    op_kwargs={
                        'zipfileName': f'{{{{ ti.xcom_pull(task_ids="batch_processing.processing_{i}.download_{i}") }}}}'
                    },
                    provide_context=True,  # Ensures context is passed for XCom access
                )
                
                download >> import_files

    search_files >> tg
    
dag_instance = daily_sentinel_batch_AARNet_processing_dag_1()