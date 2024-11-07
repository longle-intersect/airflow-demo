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

# DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

# Python function to create SLURM script
def create_slurm_script(date, stage, stage_script, **kwargs):
    script_name = f'{date}_s{stage}.slurm'
    script_content = f"""#!/bin/bash
#SBATCH --job-name=sentt_test_{date}_s{stage}
#SBATCH --output=/home/lelong/log_airflow_slurm/stdout/sentinel2_{date}_s{stage}.log
#SBATCH --error=/home/lelong/log_airflow_slurm/stdout/sentinel2_{date}_s{stage}.error
#SBATCH -n 1
####SBATCH --mem=8192
#SBATCH -t 5:00:00

# Load sdc_testing module
module load sdc_testing
if [ $? -ne 0 ]; then
    echo "Failed to load sdc_testing module."
    exit 1
fi

# Load necessary modules
module load cloud fractionalcover
if [ $? -ne 0 ]; then
    echo "Failed to load cloud_fractionalcover module."
    exit 1
fi
    
# Specify the work to be done
cd $FILESTORE_PATH/tmp_data/

{stage_script}
"""
    script_path = f'/home/airflow/slurm_scripts/{script_name}'
    with open(script_path, 'w') as file:
        file.write(script_content)

    return script_name, script_path


@dag(dag_id='sdc_sentinel_update',
     default_args=default_args,
     description='Daily Update Sentinel-2 Imagery on SDC',
     schedule_interval=None,
     start_date=days_ago(1),
     tags=['sdc', 'sentinel'])
def daily_sentinel_batch_processing_dag():

    dates = ["20241008"]  # Assuming these dates are dynamically determined elsewhere

    @task
    def monitor_slurm_job(script_name, remote_path, local_path):
        # Placeholder for SlurmJobHandlingSensor task logic
        return SlurmJobHandlingSensor(
            task_id=f'monitor_{script_name}',
            ssh_conn_id='slurm_ssh_connection',
            script_name=script_name,
            remote_path=remote_path,
            local_path=local_path,
            timeout=3600,
            poke_interval=30,
        )

    @task
    def generate_and_process(date):
        # Define all stages and their bash commands
        stage_scripts = {
            1: """
# Stage 1: Cloud fmask processing
qv_sentinel2cloud_fmask.py --toaref10 cemsre_t55hdv_{date}_ab0m5.img

if [ $? -ne 0 ]; then
    echo "Failed at stage 1: Cloud fmask processing."
    exit 1
fi
""",
            2: """
# Stage 2: Topo masks processing
qv_sentinel2topomasks.py --toaref10 cemsre_t55hdv_{date}_ab0m5.img

if [ $? -ne 0 ]; then
    echo "Failed at stage 1: Cloud fmask processing."
    exit 1
fi
""",
            3: """
# Stage 3: Surface reflectance processing
doSfcRefSentinel2.py --toaref cemsre_t55hdv_{date}_ab0m5.img

doSfcRefSentinel2.py --toaref cemsre_t55hdv_20241008_ab0m5.img
if [ $? -ne 0 ]; then
    echo "Failed at stage 3: Surface reflectance processing."
    exit 1
fi    
""",
            4: """
# Stage 4: Water index processing
qv_water_index2015.py cemsre_t55hdv_{date}_abam5.img cemsre_t55hdv_{date}_abbm5.img --omitothermasks

if [ $? -ne 0 ]; then
    echo "Failed at stage 4: Water index processing."
    exit 1
fi    
""",
            5: """
# Stage 5: Fractional cover processing
qv_fractionalcover_sentinel2.py cemsre_t55hdv_{date}_abam5.img

if [ $? -ne 0 ]; then
    echo "Failed at stage 5: Fractional cover processing."
    exit 1
fi    
"""
        }

        # Define file paths
        remote_path = '/home/lelong/log_airflow_slurm/scripts'
        local_path = '/home/airflow/slurm_scripts'
        
        # Create and execute scripts for each stage in sequence
        prev_task = None
        for stage, stage_script in stage_scripts.items():
            script_name, script_path = create_slurm_script(date, stage, stage_script)
            job_monitoring_task = monitor_slurm_job(script_name, remote_path, local_path)
            if prev_task:
                prev_task.set_downstream(job_monitoring_task)
            prev_task = job_monitoring_task

   
    # Expand tasks for each date dynamically
    for date in dates:
        generate_and_process(date)

dag_instance = dynamic_sentinel_processing_dag()
