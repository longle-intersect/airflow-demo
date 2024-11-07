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
    'start_date': datetime(2024, 11, 1),
}


@dag(dag_id='sdc_sentinel_batch_update_daily',
     default_args=default_args,
     description='Daily Update Sentinel-2 Imagery on SDC',
     schedule_interval=None,
     start_date=days_ago(1),
     tags=['sdc', 'sentinel'])
def daily_sentinel_batch_processing_dag():

    dates = ["20241008"]  # Assuming these dates are dynamically determined elsewhere

    # Combine all commands into one large script

    # Define all tasks
    # script_stage_1_path = create_slurm_script(**{'script_name': f'{dag.dag_id}_s1.slurm',
    #                                           'stage': '1',
    #                                           'stage_script': f'{script_stage_1}'})

    # Task 1: Cloud fmask processing
    @task
    def batch_cloud_fmask_processing(dates):

        s1_job_list = []

        for date in dates:
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
                remote_path='/home/lelong/log_airflow_slurm/scripts',
                local_path='/home/airflow/slurm_scripts', 
                stage_script=script_stage_1,
                #dag=dag,
                timeout=3600,
                poke_interval=30,
            )

            s1_job_list.append(cloud_fmask_processing)

        return s1_job_list


    @task
    def batch_topo_masks_processing(dates):

        s2_job_list = []

        for date in dates:
            script_name = f'sentt_{date}'
            script_stage_2="""
qv_sentinel2topomasks.py --toaref10 cemsre_t55hdv_20241008_ab0m5.img
if [ $? -ne 0 ]; then
    echo "Failed at stage 2: Topo masks processing."
    exit 1
fi
"""
            # Task 2: Topo masks processing
            topo_masks_processing = SlurmJobHandlingSensor(
                task_id=f'{script_name}_s2',
                ssh_conn_id='slurm_ssh_connection',
                script_name=f'{dag.dag_id}_s2.slurm',
                remote_path='/home/lelong/log_airflow_slurm/scripts',
                local_path='/home/airflow/slurm_scripts', 
                stage_script=script_stage_2,
                #dag=dag,
                timeout=3600,
                poke_interval=30,
            )

            s2_job_list.append(topo_masks_processing)

        return s2_job_list


    @task
    def batch_
    script_stage_3="""
cd $FILESTORE_PATH/tmp_data/

doSfcRefSentinel2.py --toaref cemsre_t55hdv_20241008_ab0m5.img
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
cd $FILESTORE_PATH/tmp_data/

qv_water_index2015.py cemsre_t55hdv_20241008_abam5.img cemsre_t55hdv_20241008_abbm5.img --omitothermasks
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
cd $FILESTORE_PATH/tmp_data/

qv_fractionalcover_sentinel2.py cemsre_t55hdv_20241008_abam5.img
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

dag_instance = daily_sentinel_batch_processing_dag()
