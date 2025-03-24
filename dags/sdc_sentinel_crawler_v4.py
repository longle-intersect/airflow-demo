import sys
sys.path.insert(0, '/opt/airflow/dags/repo/plugins')
sys.path.insert(0, '/opt/airflow/')
import os
from pathlib import Path
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
from airflow.operators.bash import BashOperator
from airflow.decorators import task

import logging

# Set up logging
logger = logging.getLogger("airflow.task")
remote_path='/home/lelong/crawler_slurm_jobs/'
remote_metadata_path='/home/lelong/crawler_metadata/'
local_path='/opt/airflow/slurm_script/' 
local_tmp_path='/opt/airflow/tmp_files/'
local_metadata_path ='/opt/airflow/tmp_metadata/'

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': None,
}

# Define the DAG
with DAG(
    'sdc_sentinel2_crawler_v4',
    default_args=default_args,
    description='Daily Sentinel-2 folder crawler and parser via SSH v4',
    schedule_interval=None,
    start_date=datetime(2025, 3, 19),
    catchup=False,
) as dag:

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
            local_filelist = local_tmp_path+file_list_name
            sftp_client.get(file_list, local_filelist)
            sftp_client.close()

            with open(local_filelist, 'r') as f:
                files = [line.strip() for line in f if line.strip()]

            context['ti'].xcom_push(key='num_files', value=num_files)
            context['ti'].xcom_push(key='folder_visited', value=folder_visited)
            context['ti'].xcom_push(key='file_list', value=file_list)
            context['ti'].xcom_push(key='filename_list', value=filename_list)    
            context['ti'].xcom_push(key='local_filelist', value=local_filelist)   
            context['ti'].xcom_push(key='tmp_recall', value=destdir)
            context['ti'].xcom_push(key='slurm_script_path', value=slurm_script)
            context['ti'].xcom_push(key='file_list_array', value=files)       

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

    # Task to prepare file list
    @task
    def prepare_file_list(local_filelist):
        with open(local_filelist, 'r') as f:
            files = [line.strip() for line in f if line.strip()]
        return files

    # Combined metadata parsing and transfer function using SSHHook
    @task
    def parse_and_transfer_metadata(file_path):
        #file_path = context['params']['file']
        #local_json_path = f"{local_tmp_path}/{file_path.split('/')[-1]}.json"
        #remote_json_path = f"{remote_path}/{file_path.split('/')[-1]}.json"

        # Determine input file path components
        input_path = Path(file_path)
        metadata_filename = f"{input_path.stem}_metadata.json"

        logger.info(f"Metadata Filename: {metadata_filename}")
        
        remote_metadata_file = remote_metadata_path + metadata_filename
        local_metadata_file = local_metadata_path + metadata_filename

        logger.info(f"Remote Metadata Filename: {remote_metadata_file}")
        logger.info(f"Local Metadata Filename: {local_metadata_file}")

        try:
            logger.info(f"Starting metadata parsing for {file_path}")
            ssh_hook = SSHHook(ssh_conn_id='slurm_ssh_connection')
            ssh_client = ssh_hook.get_conn()

            # Define and execute metadata parsing command
            command = (
                "cd rswms/dataops/sdc_slurm && "
                f"python history2json_v2.py {file_path} -d {remote_metadata_path}"
            )
            logger.info(f"Executing command: {command}")
            
            stdin, stdout, stderr = ssh_client.exec_command(command, timeout=300)
            exit_status = stdout.channel.recv_exit_status()
            output = stdout.read().decode('utf-8')
            error_output = stderr.read().decode('utf-8')

            if exit_status != 0:
                raise AirflowException(f"Metadata parsing failed for {file_path} with exit code {exit_status}. Error: {error_output}")

            os.makedirs(local_metadata_path, mode=0o777, exist_ok=True)
            # Transfer JSON file back to Airflow worker
            sftp_client = ssh_client.open_sftp()
            sftp_client.get(remote_metadata_file, local_metadata_file)
            sftp_client.close()

            #context['ti'].xcom_push(key=f'json_path_{file_path.split("/")[-1]}', value=local_metadata_file)
            logger.info(f"Successfully parsed and transferred metadata for {file_path}")

        except Exception as e:
            logger.error(f"Failed to process {file_path}: {str(e)}", exc_info=True)
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

    # Workflow with dynamic task mapping
    file_list = prepare_file_list(local_filelist="{{ ti.xcom_pull(task_ids='crawl_files', key='local_filelist') }}")
    parse_results = parse_and_transfer_metadata.expand(file_path=file_list)

    crawl_files >> recall_files >> file_list >> parse_results