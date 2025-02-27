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
from slurm_job_handler_lsat import SlurmJobHandlingSensor
from airflow.utils.task_group import TaskGroup

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
remote_path='/home/lelong/log_airflow_slurm/scripts/'
local_path='/opt/airflow/slurm_scripts/' 

# DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2025, 1, 22),
}


@dag(dag_id='sdc_landsat_batch_daily_taskgroup',
     default_args=default_args,
     description='Daily Update Landsat Imagery TaskGroup on SDC',
     schedule_interval=None,
     start_date=days_ago(1),
     tags=['sdc', 'landsat'])
def daily_landsat_batch_processing_dag():

    dates = ["l9olre_p089r079_20250114",
             "l9olre_p089r084_20250114",
             "l9olre_p089r085_20250114"
            ]  # Assuming these dates are dynamically determined elsewhere

    # get_list = PythonOperator(task_id="get_img_list",
    #                           python_callable=get_dates,
    #                           do_xcom_push=True)

    # Combine all commands into one large script
    for i, date in enumerate(dates):
        prefix = f"lsat_{date}"
        with TaskGroup(group_id=f'process_img_{i}') as tg:
            # Task 1: makeincidenceangles
            make_incidence_angles = SlurmJobHandlingSensor(
                task_id=f'i{i}_s1',
                ssh_conn_id='slurm_ssh_connection',
                script_name=f'{prefix}_s1',
                remote_path=remote_path,
                local_path=local_path, 
                #stage_script=script_stage_1,
                #dag=dag,
                timeout=3600,
                poke_interval=30,
                date = date,
                stage = "1"
            )

            # Task 2: toa_brdf_reflectance
            toa_brdf_reflectance = SlurmJobHandlingSensor(
                task_id=f'i{i}_s2',
                ssh_conn_id='slurm_ssh_connection',
                script_name=f'{prefix}_s2',
                remote_path=remote_path,
                local_path=local_path, 
                #stage_script=script_stage_1,
                #dag=dag,
                timeout=3600,
                poke_interval=30,
                date = date,
                stage = "2",       
            )


            # Task 3: topocorrectref
            topocorrectref = SlurmJobHandlingSensor(
                task_id=f'i{i}_s3',
                ssh_conn_id='slurm_ssh_connection',
                script_name=f'{prefix}_s3',
                remote_path=remote_path,
                local_path=local_path, 
                #stage_script=script_stage_1,
                #dag=dag,
                timeout=3600,
                poke_interval=30,
                date = date,
                stage = "3",      
            )

            # Task 4:incidencemask
            incidencemask = SlurmJobHandlingSensor(
                task_id=f'i{i}_s4',
                ssh_conn_id='slurm_ssh_connection',
                script_name=f'{prefix}_s4',
                remote_path=remote_path,
                local_path=local_path, 
                #stage_script=script_stage_1,
                #dag=dag,
                timeout=3600,
                poke_interval=30,
                date = date,
                stage = "4",      
            )

            # Task 5: castshadowmask
            castshadowmask = SlurmJobHandlingSensor(
                task_id=f'i{i}_s5',
                ssh_conn_id='slurm_ssh_connection',
                script_name=f'{prefix}_s5',
                remote_path=remote_path,
                local_path=local_path, 
                #stage_script=script_stage_1,
                #dag=dag,
                timeout=3600,
                poke_interval=30,
                date = date,
                stage = "5",      
            )

            # Task 6: fpc_topocorrected
            fpc_topocorrected = SlurmJobHandlingSensor(
                task_id=f'i{i}_s6',
                ssh_conn_id='slurm_ssh_connection',
                script_name=f'{prefix}_s6',
                remote_path=remote_path,
                local_path=local_path, 
                #stage_script=script_stage_1,
                #dag=dag,
                timeout=3600,
                poke_interval=30,
                date = date,
                stage = "6",      
            )

            # Task 7: temperature
            temperature = SlurmJobHandlingSensor(
                task_id=f'i{i}_s7',
                ssh_conn_id='slurm_ssh_connection',
                script_name=f'{prefix}_s7',
                remote_path=remote_path,
                local_path=local_path, 
                #stage_script=script_stage_1,
                #dag=dag,
                timeout=3600,
                poke_interval=30,
                date = date,
                stage = "7",      
            )

            # Task 8: watermask_topocorrected
            watermask_topocorrected = SlurmJobHandlingSensor(
                task_id=f'i{i}_s8',
                ssh_conn_id='slurm_ssh_connection',
                script_name=f'{prefix}_s8',
                remote_path=remote_path,
                local_path=local_path, 
                #stage_script=script_stage_1,
                #dag=dag,
                timeout=3600,
                poke_interval=30,
                date = date,
                stage = "8",      
            )

            # Task 9: surfacereflectance
            surfacereflectance = SlurmJobHandlingSensor(
                task_id=f'i{i}_s9',
                ssh_conn_id='slurm_ssh_connection',
                script_name=f'{prefix}_s9',
                remote_path=remote_path,
                local_path=local_path, 
                #stage_script=script_stage_1,
                #dag=dag,
                timeout=3600,
                poke_interval=30,
                date = date,
                stage = "9",      
            )

            # Task 10: ndvi
            ndvi = SlurmJobHandlingSensor(
                task_id=f'i{i}_s10',
                ssh_conn_id='slurm_ssh_connection',
                script_name=f'{prefix}_s10',
                remote_path=remote_path,
                local_path=local_path, 
                #stage_script=script_stage_1,
                #dag=dag,
                timeout=3600,
                poke_interval=30,
                date = date,
                stage = "10",      
            )

            # Task 11: fmaskcloud
            fmaskcloud = SlurmJobHandlingSensor(
                task_id=f'i{i}_s11',
                ssh_conn_id='slurm_ssh_connection',
                script_name=f'{prefix}_s11',
                remote_path=remote_path,
                local_path=local_path, 
                #stage_script=script_stage_1,
                #dag=dag,
                timeout=3600,
                poke_interval=30,
                date = date,
                stage = "11",      
            )

            # Task 12: fractionalcover_sfcref
            fractionalcover_sfcref = SlurmJobHandlingSensor(
                task_id=f'i{i}_s12',
                ssh_conn_id='slurm_ssh_connection',
                script_name=f'{prefix}_s12',
                remote_path=remote_path,
                local_path=local_path, 
                #stage_script=script_stage_1,
                #dag=dag,
                timeout=3600,
                poke_interval=30,
                date = date,
                stage = "12",      
            )

            # Task 13: binarywatermask
            binarywatermask = SlurmJobHandlingSensor(
                task_id=f'i{i}_s13',
                ssh_conn_id='slurm_ssh_connection',
                script_name=f'{prefix}_s13',
                remote_path=remote_path,
                local_path=local_path, 
                #stage_script=script_stage_1,
                #dag=dag,
                timeout=3600,
                poke_interval=30,
                date = date,
                stage = "13",      
            )

            # Task 14: waterindex_2015
            waterindex_2015 = SlurmJobHandlingSensor(
                task_id=f'i{i}_s14',
                ssh_conn_id='slurm_ssh_connection',
                script_name=f'{prefix}_s14',
                remote_path=remote_path,
                local_path=local_path, 
                #stage_script=script_stage_1,
                #dag=dag,
                timeout=3600,
                poke_interval=30,
                date = date,
                stage = "14",      
            )

            # Task Dependency Setup
            make_incidence_angles >> toa_brdf_reflectance >> topocorrectref \
                  >> incidencemask >> castshadowmask >> fpc_topocorrected \
                  >> temperature >> watermask_topocorrected >> surfacereflectance \
                  >> ndvi >> fmaskcloud >> fractionalcover_sfcref >> binarywatermask >> waterindex_2015

dag_instance = daily_landsat_batch_processing_dag()
