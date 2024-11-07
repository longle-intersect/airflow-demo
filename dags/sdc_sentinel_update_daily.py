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
remote_path='/home/lelong/log_airflow_slurm/scripts',
local_path='/home/airflow/slurm_scripts', 

# DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 11, 1),
}


@dag(dag_id='sdc_sentinel_batch_update_daily',
     default_args=default_args,
     description='Daily Update Sentinel-2 Imagery on SDC',
     schedule_interval=None,
     start_date=days_ago(1),
     tags=['sdc', 'sentinel'])
def daily_sentinel_batch_processing_dag():

    dates = ["20241008", "20241001"]  # Assuming these dates are dynamically determined elsewhere

    # Combine all commands into one large script

    # Define all tasks
    # script_stage_1_path = create_slurm_script(**{'script_name': f'{dag.dag_id}_s1.slurm',
    #                                           'stage': '1',
    #                                           'stage_script': f'{script_stage_1}'})

    # Task 1: Cloud fmask processing
    @task
    def batch_cloud_fmask_processing(date):

        #s1_job_list = []

        #for date in dates:

        script_name = f'sentt_{date}'
        script_stage_1 = """
# Execute cloud fmask processing
qv_sentinel2cloud_fmask.py --toaref10 cemsre_t55hdv_{date}_ab0m5.img
if [ $? -ne 0 ]; then
echo "Failed at stage 1: Cloud fmask processing."
exit 1
fi
"""    
        cloud_fmask_processing = SlurmJobHandlingSensor(
            task_id=f'{script_name}_s1',
            ssh_conn_id='slurm_ssh_connection',
            script_name=f'{script_name}_s1.slurm',
            remote_path=remote_path,
            local_path=local_path, 
            stage_script=script_stage_1,
            #dag=dag,
            timeout=3600,
            poke_interval=30,
        )

        #s1_job_list.append(cloud_fmask_processing)

        return date


    @task
    def batch_topo_masks_processing(date):

        #for date in dates:
        script_name = f'sentt_{date}'
        script_stage_2="""
qv_sentinel2topomasks.py --toaref10 cemsre_t55hdv_{date}_ab0m5.img
if [ $? -ne 0 ]; then
    echo "Failed at stage 2: Topo masks processing."
    exit 1
fi
"""
        # Task 2: Topo masks processing
        topo_masks_processing = SlurmJobHandlingSensor(
            task_id=f'{script_name}_s2',
            ssh_conn_id='slurm_ssh_connection',
            script_name=f'{script_name}_s2.slurm',
            remote_path=remote_path,
            local_path=local_path, 
            stage_script=script_stage_2,
            #dag=dag,
            timeout=3600,
            poke_interval=30,
        )

        return date


    @task
    def batch_surface_reflectance_processing(date):

        script_name = f'sentt_{date}'
        script_stage_3="""
doSfcRefSentinel2.py --toaref cemsre_t55hdv_{date}_ab0m5.img
if [ $? -ne 0 ]; then
    echo "Failed at stage 3: Surface reflectance processing."
    exit 1
fi    
"""

        # Task 3: Surface reflectance processing
        surface_reflectance_processing = SlurmJobHandlingSensor(
            task_id=f'{script_name}_s3',
            ssh_conn_id='slurm_ssh_connection',
            script_name=f'{script_name}_s3.slurm',
            remote_path=remote_path,
            local_path=local_path, 
            stage_script=script_stage_3,        
            #dag=dag,
            timeout=3600,
            poke_interval=30,
        )

        return date

    @task
    def batch_water_index_processing(date):

        script_name = f'sentt_{date}'
        script_stage_4="""
qv_water_index2015.py cemsre_t55hdv_{date}_abam5.img cemsre_t55hdv_{date}_abbm5.img --omitothermasks
if [ $? -ne 0 ]; then
    echo "Failed at stage 4: Water index processing."
    exit 1
fi    
"""
       # Task 4: Water index processing
        water_index_processing = SlurmJobHandlingSensor(
            task_id=f'{script_name}_s4',
            ssh_conn_id='slurm_ssh_connection',
            script_name=f'{script_name}_s4.slurm',
            remote_path=remote_path,
            local_path=local_path,
            stage_script=script_stage_4,         
            #dag=dag,
            timeout=3600,
            poke_interval=30,
        )

        return date

    @task
    def batch_fractional_cover_processing(date):

        script_name = f'sentt_{date}'
        script_stage_5="""
qv_fractionalcover_sentinel2.py cemsre_t55hdv_{date}_abam5.img
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
            task_id=f'{script_name}_s5',
            ssh_conn_id='slurm_ssh_connection',
            script_name=f'{script_name}_s5.slurm',
            remote_path=remote_path,
            local_path=local_path,
            stage_script=script_stage_5,        
            #dag=dag,
            timeout=3600,
            poke_interval=30,
        )

        return date

    # Task Dependency Setup
    #cloud_fmask_processing >> topo_masks_processing >> surface_reflectance_processing >> water_index_processing >> fractional_cover_processing

    s1_dates = batch_cloud_fmask_processing.expand(date=dates)
    s2_dates = batch_topo_masks_processing.expand(date=s1_dates)
    s3_dates = batch_surface_reflectance_processing.expand(date=s2_dates)
    s4_dates = batch_water_index_processing.expand(date=s3_dates)
    s5_dates = batch_fractional_cover_processing.expand(date=s4_dates)

dag_instance = daily_sentinel_batch_processing_dag()
