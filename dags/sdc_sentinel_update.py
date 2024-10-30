import sys
sys.path.insert(0, '/opt/airflow/dags/repo/plugins')
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from slurm_job_handler_new import SlurmJobHandlingSensor

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
def create_slurm_script(**kwargs):
    script_content = f"""#!/bin/bash
#SBATCH --job-name=sentt_test_s{kwargs['stage']}
#SBATCH --output=/home/lelong/log_airflow_slurm/stdout/sentinel2_stage_{kwargs['stage']}.log
#SBATCH --error=/home/lelong/log_airflow_slurm/stdout/sentinel2_stage_{kwargs['stage']}.error
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
{kwargs['stage_script']}
"""
    script_path = f'/home/airflow/slurm_scripts/{kwargs['script_name']}'
    with open(script_path, 'w') as file:
        file.write(script_content)

    return script_path


with DAG(
    'sdc_sentinel_update',
    default_args=default_args,
    description='Processing Sentinel Images on SDC',
    schedule_interval=None,  # Set to None for manual triggering
    start_date=days_ago(1),
    tags=['sdc', 'sentinel'],
) as dag:

    # Combine all commands into one large script
    script_stage_1 = """
    # Execute cloud fmask processing
    qv_sentinel2cloud_fmask.py --toaref10 $FILESTORE_PATH/tmp_data/cemsre_t55hdv_20241008_ab0m5.img --updatedatabase
    if [ $? -ne 0 ]; then
        echo "Failed at stage 1: Cloud fmask processing."
        exit 1
    fi
    """

    # Define all tasks
    # script_stage_1_path = create_slurm_script(**{'script_name': f'{dag.dag_id}_s1.slurm',
    #                                           'stage': '1',
    #                                           'stage_script': f'{script_stage_1}'})

    # Task 1: Cloud fmask processing
    cloud_fmask_processing = SlurmJobHandlingSensor(
        task_id='cloud_fmask_processing',
        ssh_conn_id='slurm_ssh_connection',
        script_name=f'{dag.dag_id}_s1.slurm',
        remote_path='/home/lelong/log_airflow_slurm/scripts',
        local_path='/home/airflow/slurm_scripts', 
        stage_script=script_stage_1,
        dag=dag,
        timeout=3600,
        poke_interval=30,
    )

    script_stage_2="""
    qv_sentinel2topomasks.py --toaref10 $FILESTORE_PATH/tmp_data/cemsre_t55hdv_20241008_ab0m5.img
    if [ $? -ne 0 ]; then
        echo "Failed at stage 2: Topo masks processing."
        exit 1
    fi
    """

    # Define all tasks
    # script_stage_2_path = create_slurm_script(**{'script_name': f'{dag.dag_id}_s2.slurm',
    #                                           'stage': '2',
    #                                           'stage_script': f'{script_stage_2}'})

    # Task 2: Topo masks processing
    topo_masks_processing = SlurmJobHandlingSensor(
        task_id='topo_masks_processing',
        ssh_conn_id='slurm_ssh_connection',
        script_name=f'{dag.dag_id}_s2.slurm',
        remote_path='/home/lelong/log_airflow_slurm/scripts',
        local_path='/home/airflow/slurm_scripts', 
        stage_script=script_stage_2,
        dag=dag,
        timeout=3600,
        poke_interval=30,
    )

    script_stage_3="""
    doSfcRefSentinel2.py --toaref $FILESTORE_PATH/tmp_data/cemsre_t55hdv_20241008_ab0m5.img
    if [ $? -ne 0 ]; then
        echo "Failed at stage 3: Surface reflectance processing."
        exit 1
    fi    
    """
    # Define all tasks
    # script_stage_3_path = create_slurm_script(**{'script_name': f'{dag.dag_id}_s3.slurm',
    #                                           'stage': '3',
    #                                           'stage_script': f'{script_stage_3}'})
    # Task 3: Surface reflectance processing
    surface_reflectance_processing = SlurmJobHandlingSensor(
        task_id='surface_reflectance_processing',
        ssh_conn_id='slurm_ssh_connection',
        script_name=f'{dag.dag_id}_s3.slurm',
        remote_path='/home/lelong/log_airflow_slurm/scripts',
        local_path='/home/airflow/slurm_scripts', 
        stage_script=script_stage_3,        
        dag=dag,
        timeout=3600,
        poke_interval=30,
    )

    script_stage_4="""
    qv_water_index2015.py $FILESTORE_PATH/tmp_data/cemsre_t55hdv_20241008_abam5.img $FILESTORE_PATH/tmp_data/cemsre_t55hdv_20241008_abbm5.img --omitothermasks
    if [ $? -ne 0 ]; then
        echo "Failed at stage 4: Water index processing."
        exit 1
    fi    
    """
    # Define all tasks
    # script_stage_4_path = create_slurm_script(**{'script_name': f'{dag.dag_id}_s4.slurm',
    #                                           'stage': '4',
    #                                           'stage_script': f'{script_stage_4}'})
    # Task 4: Water index processing
    water_index_processing = SlurmJobHandlingSensor(
        task_id='water_index_processing',
        ssh_conn_id='slurm_ssh_connection',
        script_name=f'{dag.dag_id}_s4.slurm',
        remote_path='/home/lelong/log_airflow_slurm/scripts',
        local_path='/home/airflow/slurm_scripts',
        stage_script=script_stage_4,         
        dag=dag,
        timeout=3600,
        poke_interval=30,
    )

    script_stage_5="""
    qv_fractionalcover_sentinel2.py $FILESTORE_PATH/tmp_data/cemsre_t55hdv_20241008_abam5.img
    if [ $? -ne 0 ]; then
        echo "Failed at stage 5: Fractional cover processing."
        exit 1
    fi    
    """
    # Define all tasks
    # script_stage_5_path = create_slurm_script(**{'script_name': f'{dag.dag_id}_s5.slurm',
    #                                           'stage': '5',
    #                                           'stage_script': f'{script_stage_5}'})
    # Task 8: Fractional cover processing
    fractional_cover_processing = SlurmJobHandlingSensor(
        task_id='fractional_cover_processing',
        ssh_conn_id='slurm_ssh_connection',
        script_name=f'{dag.dag_id}_s5.slurm',
        remote_path='/home/lelong/log_airflow_slurm/scripts',
        local_path='/home/airflow/slurm_scripts', 
        stage_script=script_stage_5,        
        dag=dag,
        timeout=3600,
        poke_interval=30,
    )

    # Task Dependency Setup
    cloud_fmask_processing >> topo_masks_processing >> surface_reflectance_processing >> water_index_processing >> fractional_cover_processing

