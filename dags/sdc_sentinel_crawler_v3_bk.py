import sys
sys.path.insert(0, '/opt/airflow/dags/repo/plugins')
sys.path.insert(0, '/opt/airflow/')
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.exceptions import AirflowException
from plugins.slurm_job_handler_general import GeneralSlurmJobHandlingSensor
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain
from airflow.models import XCom, Variable
from airflow.providers.ssh.operators.ssh import SSHOperator

import logging

# Set up logging
logger = logging.getLogger("airflow.task")
remote_path='/home/lelong/crawler_slurm_jobs'
local_path='/opt/airflow/slurm_script/' 
local_tmp_path='/opt/airflow/tmp_files/'

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': None,
}

# Define the DAG
dag = DAG(
    'sdc_sentinel2_crawler_v3',
    default_args=default_args,
    description='Daily Sentinel-2 folder crawler and parser via SSH',
    schedule_interval=None,  # Manual triggering
    start_date=datetime(2025, 3, 19),
    catchup=False,
)

# Function to execute SSH command and parse output
def ssh_command_and_parse(**context):
    try:
        logger.info("Starting SSH task execution")
        
        # Initialize SSH connection
        ssh_hook = SSHHook(ssh_conn_id='slurm_ssh_connection')
        ssh_client = ssh_hook.get_conn()

        # Define the command
        command = (
            "cd rswms/dataops/sdc_slurm && "
            "python sentinel2crawler.py 202301 202301 /home/lelong/crawler_filelists"
        )
        logger.info(f"Executing command: {command}")

        # Execute the command
        stdin, stdout, stderr = ssh_client.exec_command(command, timeout=300)  # 5-minute timeout
        
        # Wait for the command to complete and get exit status
        exit_status = stdout.channel.recv_exit_status()
        output = stdout.read().decode('utf-8')
        error_output = stderr.read().decode('utf-8')

        logger.info(f"Command exit status: {exit_status}")
        logger.info(f"Command output: {output}")
        if error_output:
            logger.error(f"Command stderr: {error_output}")

        # Check for command failure
        if exit_status != 0:
            raise AirflowException(f"SSH command failed with exit code {exit_status}. Error: {error_output}")

        # Process the output
        lines = output.split('\n')
        num_files = None
        folder_visited = None
        for line in lines:
            if line.startswith('XCOM_NUM_FILES:'):
                num_files = int(line.replace('XCOM_NUM_FILES:', '').strip())
            elif line.startswith('XCOM_FOLDER_VISITED:'):
                folder_visited = line.replace('XCOM_FOLDER_VISITED:', '').strip()
            elif line.startswith('XCOM_FILE_TXT:'):
                file_list = line.replace('XCOM_FILE_TXT:', '').strip()
            elif line.startswith('XCOM_FILENAME_TXT:'):
                filename_list = line.replace('XCOM_FILENAME_TXT:', '').strip()
            elif line.startswith('XCOM_DESTDIR:'):
                destdir = line.replace('XCOM_DESTDIR:', '').strip()
            elif line.startswith('XCOM_SLURM_SCRIPT:'):
                slurm_script = line.replace('XCOM_SLURM_SCRIPT:', '').strip()
        # Push results to XCom
        file_list_name = file_list.split('/')[-1]

        os.makedirs(local_tmp_path, mode=0o777, exist_ok=True)
        sftp_client = ssh_client.open_sftp()
        #sftp_client.get(filename_list, local_tmp_path)
        sftp_client.get(file_list, local_tmp_path+file_list_name)
        sftp_client.close()

        context['ti'].xcom_push(key='num_files', value=num_files)
        context['ti'].xcom_push(key='folder_visited', value=folder_visited)
        context['ti'].xcom_push(key='file_list', value=file_list)
        context['ti'].xcom_push(key='filename_list', value=filename_list)    
        context['ti'].xcom_push(key='local_filelist', value=local_tmp_path + file_list_name)   
        context['ti'].xcom_push(key='tmp_recall', value=destdir)
        context['ti'].xcom_push(key='slurm_script_path', value=slurm_script)       

        logger.info(f"Number of files: {num_files}")
        logger.info(f"Folder visited: {folder_visited}")
        logger.info(f"File list: {file_list}")
        logger.info(f"File name list: {filename_list}")
        logger.info(f"Recall Location: {destdir}")
        logger.info(f"Slurm Script Path: {slurm_script}")


    except Exception as e:
        logger.error(f"Task failed with exception: {str(e)}", exc_info=True)
        raise AirflowException(f"Task execution failed: {str(e)}")
    finally:
        if 'ssh_client' in locals():
            logger.info("Closing SSH connection")
            ssh_client.close()

# Function to split file list and prepare for mapping
def prepare_file_list(**context):
    local_filelist = context['ti'].xcom_pull(task_ids='crawl_files', key='local_filelist')
    with open(local_filelist, 'r') as f:
        files = [line.strip() for line in f if line.strip()]
    
    # Push individual files to XCom for mapping
    context['ti'].xcom_push(key='file_list_array', value=files)
    return len(files)

# Define the metadata parsing function
def parse_metadata(**context):
    try:
        logger.info("Starting metadata parsing task")
        
        # Get file list from previous task
        local_filelist = context['ti'].xcom_pull(task_ids='crawl_files', key='local_filelist')
        remote_filelist = f"{remote_path}/filelist_{context['run_id']}.txt"
        
        # Initialize SSH connection
        ssh_hook = SSHHook(ssh_conn_id='slurm_ssh_connection')
        ssh_client = ssh_hook.get_conn()

        # Define the metadata parsing command
        metadata_script = "python history2json_v2.py"
        command = (
            "cd rswms/dataops/sdc_slurm && "
            f"{metadata_script} --input {remote_filelist} "
            f"--output {remote_path}/metadata_output_{context['run_id']}.json"
        )
        
        logger.info(f"Executing metadata parsing command: {command}")
        
        # Execute the command
        stdin, stdout, stderr = ssh_client.exec_command(command, timeout=600)  # 10-minute timeout
        exit_status = stdout.channel.recv_exit_status()
        output = stdout.read().decode('utf-8')
        error_output = stderr.read().decode('utf-8')

        if exit_status != 0:
            raise AirflowException(f"Metadata parsing failed with exit code {exit_status}. Error: {error_output}")

        # Optionally store results
        context['ti'].xcom_push(key='metadata_output', 
                              value=f"{remote_path}/metadata_output_{context['run_id']}.json")
        
        logger.info("Metadata parsing completed successfully")
        
    except Exception as e:
        logger.error(f"Metadata parsing failed: {str(e)}", exc_info=True)
        raise AirflowException(f"Task execution failed: {str(e)}")
    finally:
        if 'ssh_client' in locals():
            ssh_client.close()

# Define the task
crawl_files = PythonOperator(
    task_id='crawl_files',
    python_callable=ssh_command_and_parse,
    provide_context=True,
    dag=dag,
)


recall_files = GeneralSlurmJobHandlingSensor(
    task_id='recall_files',
    ssh_conn_id='slurm_ssh_connection',
    script_path="{{ ti.xcom_pull(task_ids='crawl_files', key='slurm_script_path') }}",
    remote_path=remote_path,
    local_path=local_path, 
    timeout=3600,
    poke_interval=30,
    dag=dag,
)

ssh_command = (
    ""
    #"python history2json.py "
)

extract_metadata = SSHOperator(
    task_id='extract_metadata',
    ssh_conn_id='slurm_ssh_connection',
    command=ssh_command,
    dag=dag,
    do_xcom_push=True,
)

convert_STAC = SSHOperator(
    task_id='convert_stac',
    ssh_conn_id='slurm_ssh_connection',
    command=ssh_command,
    dag=dag,
    do_xcom_push=True,
)

import_pgSTAC = SSHOperator(
    task_id='import_pgSTAC',
    ssh_conn_id='slurm_ssh_connection',
    command=ssh_command,
    dag=dag,
    do_xcom_push=True,
)

crawl_files >> recall_files >> extract_metadata >> convert_STAC >> import_pgSTAC