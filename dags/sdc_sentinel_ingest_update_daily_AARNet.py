import sys
import logging
import os
sys.path.insert(0, '/opt/airflow/dags/repo/plugins')
sys.path.insert(0, '/opt/airflow/')
import re
from datetime import datetime, timedelta
import subprocess
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed

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
remote_path='/home/lelong/log_airflow_slurm/scripts/'
local_path='/opt/airflow/slurm_script/' 
local_tmp_path='/opt/airflow/tmp_files/'
local_metadata_path ='/opt/airflow/tmp_metadata/'
shared_dir = "/mnt/scratch_lustre/tmp/rs_testing/tmp_shared/"  # Shared filestore between HPC and AARNet

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

# Function to execute SSH command and parse output
def searching(**context):
    try:
        logger.info("Starting SSH task execution")
        
        # Initialize SSH connection
        ssh_hook = SSHHook(ssh_conn_id='slurm_ssh_connection')
        ssh_client = ssh_hook.get_conn()

        # Define the command
        command = (
            f'cd {shared_dir} &&'
            'python ~/workspace/updateSentinel_fromSara_new.py --task search --sentinel 2 --regionofinterest $RSC_SENTINEL2_DFLT_REGIONOFINTEREST --startdate 2025-03-21 --numdownloadthreads 4  --logdownloadspeed --saraparam "processingLevel=L1C"'
            #'chmod 777 /mnt/scratch_lustre/tmp/rs_testing/tmp_shared/sara_urls.txt'
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
        url_list = None
        url_file = None
        for line in lines:
            if line.startswith('XCOM_URL_LIST:'):
                url_list = line.replace('XCOM_URL_LIST:', '').strip()
            elif line.startswith('XCOM_URL_FILE:'):
                url_file = line.replace('XCOM_URL_FILE:', '').strip()
 
        # Push results to XCom
        file_list_name = url_file.split('/')[-1]

        os.makedirs(local_tmp_path, mode=0o777, exist_ok=True)
        sftp_client = ssh_client.open_sftp()
        #sftp_client.get(filename_list, local_tmp_path)
        remote_filelist = shared_dir + file_list_name
        local_filelist = local_tmp_path + file_list_name
        sftp_client.get(remote_filelist, local_filelist)
        sftp_client.close()

        # with open(local_filelist, 'r') as f:
        #     files = [line.strip() for line in f if line.strip()]

        context['ti'].xcom_push(key='url_list', value=url_list)
        context['ti'].xcom_push(key='url_file', value=url_file)    

        logger.info(f"URL List: {url_list}")
        logger.info(f"URL File: {url_file}")

    except Exception as e:
        logger.error(f"Task failed with exception: {str(e)}", exc_info=True)
        raise AirflowException(f"Task execution failed: {str(e)}")
    finally:
        if 'ssh_client' in locals():
            logger.info("Closing SSH connection")
            ssh_client.close()

def download_url(**context):

    url_idx = int(context['task'].task_id.split('_')[-1])  # Extract index from task_id
    urls = context['ti'].xcom_pull(task_ids='search_new_images', key='url_list')


    if not urls or url_idx >= len(urls):
        logger.info(f"No URL available for index {url_idx}. Skipping task.")
        context['ti'].xcom_push(key=f'uri_{url_idx}', value=None)
        context['ti'].xcom_push(key=f'download_status_{url_idx}', value="Skipped")
        return None

    url = urls[url_idx]
    logger.info(f"Starting download for {url}")

    aarnet_hook = SSHHook(ssh_conn_id='aarnet_ssh_connection')

    curl_cmd_filename = f"cd {shared_dir} && curl --silent -n --location --head {url}"
    curl_cmd = f"cd {shared_dir} && curl -n -L -O -J --silent --show-error {url}"
    
    uri = None
    # Execute curl on AARNet using SSHHook
    try:
        ssh_client = aarnet_hook.get_conn()
        stdin, stdout, stderr = ssh_client.exec_command(curl_cmd_filename)
        exit_status = stdout.channel.recv_exit_status()  # Wait for command to complete
        stdout_output = stdout.read().decode('utf-8').strip()
        stderr_output = stderr.read().decode('utf-8').strip()
        
        logger.info(f"HEAD stdout: {stdout_output}")
        
        # Parse the Location header to get the final URI
        for line in stdout_output.split('\n'):
            if line.startswith('Location: '):
                uri = line.split(' ', 1)[1].strip()
                break

        if exit_status != 0 or stderr_output or not uri:
            logger.error(f"Failed to get filename for {url}: {stderr_output}")
            context['ti'].xcom_push(key=f'uri_{url_idx}', value=None)
            return None

        context['ti'].xcom_push(key=f'uri_{url_idx}', value=uri)
        logger.info(f"Resolved URI: {uri}")
    except Exception as e:
        logger.error(f"Error in HEAD request for {url}: {str(e)}")
        return None
    finally:
        ssh_client.close()

    # Execute curl on AARNet using SSHHook
    try:
        ssh_client = aarnet_hook.get_conn()
        stdin, stdout, stderr = ssh_client.exec_command(curl_cmd)
        exit_status = stdout.channel.recv_exit_status()  # Wait for command to complete
        stderr_output = stderr.read().decode().strip()
        
        if exit_status != 0 or stderr_output:
            logger.error(f"Failed to download {uri}: {stderr_output}")
            context['ti'].xcom_push(key=f'download_status_{url_idx}', value="Failed")
            return None
        
        logger.info(f"Successfully downloaded {uri} to {shared_dir}")
        context['ti'].xcom_push(key=f'download_status_{url_idx}', value="Success")
        return uri
    except Exception as e:
        logger.error(f"Error downloading {url}: {str(e)}")
        context['ti'].xcom_push(key=f'download_status_{url_idx}', value="Failed")
        return None
    finally:
        ssh_client.close()


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


@dag(dag_id='sdc_sentinel_batch_ingest_update_daily_AARNet',
     default_args=default_args,
     description='Daily Ingest with AARNet and Update Sentinel-2 Imagery using TaskGroup 4 on SDC',
     schedule_interval=None,
     start_date=days_ago(1),
     concurrency = 4,
     tags=['sdc', 'sentinel', 'daily'])
def daily_sentinel_batch_AARNet_processing_dag():

    # Define the task
    search_files = PythonOperator(
        task_id='search_new_images',
        python_callable=searching,
        provide_context=True,
        #dag=dag,
    )

    # Function to create TaskGroup for batch processing
    max_tasks = 4  # Adjust based on expected max URLs
    @task_group(group_id="batch_processing")
    def create_batch_tasks():
        #urls = context['ti'].xcom_pull(task_ids='search_new_images', key='url_list')
        # urls_file = os.path.join(local_tmp_path, "sara_urls.txt")
        
        # # Read URLs from file
        # with open(urls_file, 'r') as f:
        #     urls = [line.strip() for line in f.readlines() if line.strip()]

        #if not urls:
        #    logger.info("No URLs to process.")
        #    return
        
       # with TaskGroup(group_id='batch_processing', tooltip="Batch Download and Import Tasks") as batch_group:
        #for idx, url in enumerate(urls):
        for idx in range(max_tasks):
                #filename = url.split('/')[-1]
                #file_path = os.path.join(shared_dir, filename)
            @task_group(group_id=f'download_import_{idx}', tooltip="Batch Download and Import Tasks")
            def download_and_import_url():     # Define the task
                download_task = PythonOperator(
                    task_id=f'download_{idx}',
                    python_callable=download_url,
                    #op_kwargs={'url': url},
                    provide_context=True,
                    do_xcom_push=True,
                    #dag=dag,
                )

                # Import task on HPC
                # import_task = SSHOperator(
                #     task_id=f'import_{idx}',
                #     ssh_conn_id='slurm_ssh_connection',
                #     command=f"module load sdc_testing && cd {shared_dir} && python ~/workspace/updateSentinel_fromSara_new.py --task import --sentinel 2 --inputfile {url} --shared_dir {shared_dir}",
                # )
                
                # Set dependency within each batch
                download_task #>> import_task

            download_and_import_url()
    
    search_files >> create_batch_tasks()

dag_instance = daily_sentinel_batch_AARNet_processing_dag()

    # Batch processing task
    # download_and_import = PythonOperator(
    #     task_id='create_batch_tasks',
    #     python_callable=create_batch_tasks,
    #     provide_context=True,
    # )
    # download_files = SSHOperator(
    #     task_id='download_files',
    #     ssh_conn_id= 'aarnet_ssh_connection',
    #     command="""
    #     curl -n -L -O -J --silent --show-error "{{ ti.xcom_pull(task_ids='search_new_files', key='url_list') }}"
    #     """,
    #     conn_timeout=3600,
    #     cmd_timeout=3600,
    #     do_xcom_push=True  # Pushes the command output to XCom
    # )

    # import_files = PythonOperator(
    #     task_id='import_files',
    #     python_callable=importing,
    #     provide_context=True,
    #     #dag=dag,
    # )
    
    # get_new_list = PythonOperator(
    #     task_id="get_new_list",
    #     python_callable=parse_file_list,
    # )

    # @task_group(group_id='batch_processing')
    # def process_date_group():
    #     mapped_args = Variable.get("new_list", default_var=[], deserialize_json=True)

    #     # Expand the dynamic task group only if mapped_args is not empty
    #     if mapped_args:
    #         for i, date in enumerate(mapped_args):
    #             @task_group(group_id=f"process_img_{i}")
    #             def dynamic_task_group_node(date):
    #                 # Create a custom task for each mapped argument
                    
    #                 # Task 1: Cloud fmask processing
    #                 cloud_fmask_processing = SlurmJobHandlingSensor(
    #                     task_id=f'i{i}_s1',
    #                     ssh_conn_id='slurm_ssh_connection',
    #                     script_name=f'sentt_{date}_s1',
    #                     remote_path=remote_path,
    #                     local_path=local_path, 
    #                     #stage_script=script_stage_1,
    #                     #dag=dag,
    #                     timeout=3600,
    #                     poke_interval=30,
    #                     date = date,
    #                     stage = "1"
    #                 )

    #                 # Task 2: Topo masks processing
    #                 topo_masks_processing = SlurmJobHandlingSensor(
    #                     task_id=f'i{i}_s2',
    #                     ssh_conn_id='slurm_ssh_connection',
    #                     script_name=f'sentt_{date}_s2',
    #                     remote_path=remote_path,
    #                     local_path=local_path, 
    #                     #stage_script=script_stage_1,
    #                     #dag=dag,
    #                     timeout=3600,
    #                     poke_interval=30,
    #                     date = date,
    #                     stage = "2",       
    #                 )


    #                 # Task 3: Surface reflectance processing
    #                 surface_reflectance_processing = SlurmJobHandlingSensor(
    #                     task_id=f'i{i}_s3',
    #                     ssh_conn_id='slurm_ssh_connection',
    #                     script_name=f'sentt_{date}_s3',
    #                     remote_path=remote_path,
    #                     local_path=local_path, 
    #                     #stage_script=script_stage_1,
    #                     #dag=dag,
    #                     timeout=3600,
    #                     poke_interval=30,
    #                     date = date,
    #                     stage = "3",      
    #                 )

    #                 # Task 4: Water index processing
    #                 water_index_processing = SlurmJobHandlingSensor(
    #                     task_id=f'i{i}_s4',
    #                     ssh_conn_id='slurm_ssh_connection',
    #                     script_name=f'sentt_{date}_s4',
    #                     remote_path=remote_path,
    #                     local_path=local_path, 
    #                     #stage_script=script_stage_1,
    #                     #dag=dag,
    #                     timeout=3600,
    #                     poke_interval=30,
    #                     date = date,
    #                     stage = "4",      
    #                 )

    #                 # Task 5: Fractional cover processing
    #                 fractional_cover_processing = SlurmJobHandlingSensor(
    #                     task_id=f'i{i}_s5',
    #                     ssh_conn_id='slurm_ssh_connection',
    #                     script_name=f'sentt_{date}_s5',
    #                     remote_path=remote_path,
    #                     local_path=local_path, 
    #                     #stage_script=script_stage_1,
    #                     #dag=dag,
    #                     timeout=3600,
    #                     poke_interval=30,
    #                     date = date,
    #                     stage = "5",      
    #                 )

    #                 # Task Dependency Setup
    #                 cloud_fmask_processing >> topo_masks_processing >> surface_reflectance_processing >> water_index_processing >> fractional_cover_processing

    #             dynamic_task_group_node(date)

    # # Link the start task to the task group
    # download_files >> get_new_list >> process_date_group()




def downloading(**context):
    shared_dir = "/mnt/scratch_lustre/tmp/rs_testing/tmp_shared"  # Shared filestore between HPC and AARNet
    urls_file = os.path.join(local_tmp_path, "sara_urls.txt")
    downloaded_files = os.path.join(local_tmp_path, "downloaded_files.txt")
    max_concurrent = 2  # Adjust based on AARNet capacity
    
    # Initialize SSHHook for AARNet
    aarnet_hook = SSHHook(ssh_conn_id='aarnet_ssh_connection')
    
    # Read URLs from file
    with open(urls_file, 'r') as f:
        urls = [line.strip() for line in f.readlines() if line.strip()]
    
    if not urls:
        print("No URLs to download.")
        return
    
    # Remove existing downloaded_files list
    if os.path.exists(downloaded_files):
        os.remove(downloaded_files)
    
    def download_url(url):
        filename = url.split('/')[-1]
        curl_cmd = f"cd {shared_dir} && curl -n -L -O -J --silent --show-error {url}"
        
        # Execute curl on AARNet using SSHHook
        try:
            ssh_client = aarnet_hook.get_conn()
            stdin, stdout, stderr = ssh_client.exec_command(curl_cmd)
            exit_status = stdout.channel.recv_exit_status()  # Wait for command to complete
            stderr_output = stderr.read().decode().strip()
            
            if exit_status != 0 or stderr_output:
                print(f"Failed to download {filename}: {stderr_output}")
                return None
            return filename
        finally:
            ssh_client.close()
    
    # Run downloads in parallel
    downloaded = []
    with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
        future_to_url = {executor.submit(download_url, url): url for url in urls}
        for future in as_completed(future_to_url):
            result = future.result()
            if result:
                downloaded.append(result)
    
    # Write successful downloads to file
    with open(downloaded_files, 'w') as f:
        f.write('\n'.join(downloaded))
    print(f"Downloaded {len(downloaded)} files to {shared_dir}, listed in {downloaded_files}")

