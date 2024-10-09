# example_dag.py
import sys
sys.path.insert(0, '/opt/airflow/dags/repo/plugins')
from airflow import DAG
from slurm_job_handler import SlurmJobHandlingSensor
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Python function to create SLURM script
def create_download_script(**kwargs):
    script_content = """#!/bin/bash
#SBATCH --job-name=process_images
#SBATCH --output=./job_script/test_job_output.txt
#SBATCH --error=./job_script/test_job_error.txt
#SBATCH --ntasks=1
#SBATCH --mem=4G

# Load necessary modules
# module load gdal  # Adjust the version as necessary

# Activate a Python virtual environment if you use one
# source testing_dataops/bin/activate

# Ensure all Python dependencies are installed
# pip install numpy==1.20.0 requests geopandas==0.10.2 rasterstats==0.15.0

# Define command line arguments
#json_data = "https://storage.googleapis.com/gcp-ml-88/response_1641976752143.json"
#shp_fname = "https://storage.googleapis.com/gcp-ml-88/T31TCJ_MIS_BTH"
#output_path = "/home/lelong/temp_data/sentinel2"

# Run the Python script
python /home/lelong/workspace/s2a_pipeline/sentinel2-pipeline/process_img/process_img.py --json_data https://storage.googleapis.com/gcp-ml-88/response_1641976752143.json --shp_fname https://storage.googleapis.com/gcp-ml-88/T31TCJ_MIS_BTH --output_path /home/lelong/temp_data/sentinel2
"""
    script_path = '/home/airflow/slurm_scripts/test_download_s2image.slurm'
    with open(script_path, 'w') as file:
        file.write(script_content)
    return script_path


# Python function to create SLURM script
def create_processing_script(**kwargs):
    script_content = f"""#!/bin/bash
#SBATCH --job-name=process_images
#SBATCH --output=./job_script/test_job_output.txt
#SBATCH --error=./job_script/test_job_error.txt
#SBATCH --ntasks=1
#SBATCH --mem=4G

# Load necessary modules
# module load gdal  # Adjust the version as necessary

# Activate a Python virtual environment if you use one
# source testing_dataops/bin/activate

# Ensure all Python dependencies are installed
# pip install numpy==1.20.0 requests geopandas==0.10.2 rasterstats==0.15.0

# Run the Python script
python /home/lelong/workspace/s2a_pipeline/sentinel2-pipeline/temporal_stats/temporal_stats.py --input_path {kwargs['input_path']} --output_path /home/lelong/temp_data/sentinel2_step2
"""
    script_path = '/home/airflow/slurm_scripts/test_process_s2image.slurm'
    with open(script_path, 'w') as file:
        file.write(script_content)
    return script_path

with DAG('test_download_s2image',
         default_args=default_args,
         description='A sample DAG to download S2 Imagery',
         schedule_interval=None,
         start_date=datetime(2023, 1, 1),
         catchup=False) as dag:

    create_script = PythonOperator(
        task_id='prepare_download_script',
        python_callable=create_slurm_script,
        provide_context=True,
        dag=dag,
    )

    handle_slurm_job = SlurmJobHandlingSensor(
        task_id='download_s2_imagery',
        ssh_conn_id='slurm_ssh_connection',
        script_name='test_download_s2image.slurm',
        remote_path='/home/lelong/job_script',
        local_path='/home/airflow/slurm_scripts/',
        dag=dag,
        timeout=3600,  # max time in seconds for the sensor to run
        poke_interval=30  # interval in seconds between checks
    )

    create_script >> handle_slurm_job
