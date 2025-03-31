# example_dag.py
import os
import logging
import sys
sys.path.insert(0, '/opt/airflow/dags/repo/plugins')
sys.path.insert(0, '/opt/airflow/')

from airflow import DAG
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.exceptions import AirflowException
from plugins.slurm_job_handler import SlurmJobHandlingSensor
from plugins.slurm_job_handler_gpu import SlurmJobHandlingSensorGPU
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.models import XCom, Variable


# Set up logging
logger = logging.getLogger("airflow.task")
remote_path='/home/liuyuey/slurm_script/'
local_path='/opt/airflow/slurm_script/' 

dir_path = '/mnt/scratch_lustre/tmp/rs_testing/yueyang/sdc_data/'
src_img = 'cemsre_t55hcb_20241230_ab0m5.img'
src_img = dir_path + src_img
# replace img with tif
dst_img = src_img.replace('.img', '.tif')
# add retile to the end of the dir_path
retile_dir = dir_path + 'retile/'
# add masks to the end of the dir_path
masks_dir = dir_path + 'masks/'
# add _mask to the end of the image name
mask_img = dst_img.replace('.tif', '_mask.tif')
geojson = dst_img.replace('.tif', '.geojson')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Python function to create SLURM script
def preprocess_script(**kwargs):
    script_content = f"""#!/bin/bash
#SBATCH --job-name=gdal_preprocess
#SBATCH --output={remote_path}/gdal_preprocess_output.txt
#SBATCH --error={remote_path}/gdal_preprocess_error.txt
#SBATCH -n 1
#SBATCH --mem=4G
#SBATCH -t 00:20:00

# Specify the work to be done
gdal_translate -ot Byte -scale 1 10000 0 255 -of GTiff -b 3 -b 2 -b 1 {kwargs['src']} {kwargs['dst']}
mkdir -p {kwargs['dir']}
gdal_retile.py -targetDir {kwargs['dir']} -ps 1098 1098 {kwargs['dst']}
"""
    script_path = f'{local_path}/{kwargs['script_name']}'
    with open(script_path, 'w') as file:
        file.write(script_content)
    return script_path

def inference_script(**kwargs):
    script_content = f"""#!/bin/bash
#SBATCH --job-name=samgeo_inference
#SBATCH --output={remote_path}/inference_output.txt
#SBATCH --error={remote_path}/inference_error.txt
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -c 4
#SBATCH --mem 8G
#SBATCH --account=liuyuey
#SBATCH -t 0-02:00 # time (D-HH:MM)
#SBATCH --partition=GPU
#SBATCH --gres=gpu:1   # allowed number range: 1-2
#SBATCH --nodelist=sdccomp07   # available nodes: sdccomp06,07,13

# Specify the work to be done
source /mnt/scratch_lustre/tmp/rs_testing/virtual_envs/sam/bin/activate
export HF_HOME=/mnt/scratch_lustre/tmp/rs_testing/yueyang/checkpionts
mkdir -p {kwargs['output_path']}
python /home/liuyuey/slurm_script/sam2_batch_inference.py --input_path {kwargs['input_path']} --output_path {kwargs['output_path']}
deactivate
"""
    script_path = f'{local_path}/{kwargs['script_name']}'
    with open(script_path, 'w') as file:
        file.write(script_content)
    return script_path


def merge_convert_script(**kwargs):
    script_content = f"""#!/bin/bash
#SBATCH --job-name=merge_convert
#SBATCH --output={remote_path}/merge_convert_output.txt
#SBATCH --error={remote_path}/merge_convert_error.txt
#SBATCH -n 1
#SBATCH --mem=4G
#SBATCH -t 00:20:00

# Specify the work to be done
ls -1 {kwargs['dir']}/*_mask.tif > {kwargs['dir']}/tiff_list.txt
gdal_merge.py -o {kwargs['dst']} --optfile {kwargs['dir']}/tiff_list.txt
gdal_polygonize.py -8 {kwargs['dst']} -f "GeoJSON" {kwargs['dst'].replace('.tif', '.geojson')}
"""
    script_path = f'{local_path}/{kwargs['script_name']}'
    with open(script_path, 'w') as file:
        file.write(script_content)
    return script_path


def convert2geojson_script(**kwargs):
    script_content = f"""#!/bin/bash
#SBATCH --job-name=convert2geojson
#SBATCH --output={remote_path}/merge_convert_output.txt
#SBATCH --error={remote_path}/merge_convert_error.txt
#SBATCH -n 1
#SBATCH -c 2
#SBATCH --mem=4G
#SBATCH -t 00:20:00

# Specify the work to be done
source /mnt/scratch_lustre/tmp/rs_testing/virtual_envs/sam/bin/activate
python sam2_batch_inference.py --input {kwargs['input']} --output {kwargs['output']} --source {kwargs['source']}
deactivate
"""
    script_path = f'{local_path}/{kwargs['script_name']}'
    with open(script_path, 'w') as file:
        file.write(script_content)
    return script_path



with DAG('yueyang_samgeo_inf',
         default_args=default_args,
         description='Samgeo inference with multi-step DAG',
         schedule_interval=None,
         start_date=datetime(2023, 1, 1),
         catchup=False) as dag:



    create_gdal_preprocess = PythonOperator(
        task_id='create_gdal_preprocess_script',
        python_callable=preprocess_script,
        op_kwargs={'script_name': 'gdal_preprocess.slurm',
                    'src': '/mnt/scratch_lustre/tmp/rs_testing/yueyang/sdc_data/cemsre_t55hcb_20241230_ab0m5.img',
                    'dst': '/mnt/scratch_lustre/tmp/rs_testing/yueyang/sdc_data/cemsre_t55hcb_20241230_ab0m5.tif',
                    'dir': '/mnt/scratch_lustre/tmp/rs_testing/yueyang/sdc_data/retile/'},
        provide_context=True,
        dag=dag,
    )

    handle_gdal_preprocess = SlurmJobHandlingSensor(
        task_id='handle_gdal_preprocess',
        ssh_conn_id='slurm_ssh_connection_yueyang',
        script_name='gdal_preprocess.slurm',
        remote_path=remote_path,
        local_path=local_path,
        dag=dag,
        timeout=3600,  # max time in seconds for the sensor to run
        poke_interval=30  # interval in seconds between checks
    )

    create_inference = PythonOperator(
        task_id='create_inference_script',
        python_callable=inference_script,
        op_kwargs={'script_name': 'sam_inference.slurm', 
                   'input_path': retile_dir, 
                   'output_path': masks_dir},
        provide_context=True,
        dag=dag,
    )

    handle_inference = SlurmJobHandlingSensorGPU(
        task_id='handle_inference',
        ssh_conn_id='slurm_ssh_connection_yueyang',
        script_name='sam_inference.slurm',
        remote_path=remote_path,
        local_path=local_path,
        dag=dag,
        timeout=7200,  # max time in seconds for the sensor to run
        poke_interval=30  # interval in seconds between checks
    )

    create_merge_convert = PythonOperator(
        task_id='create_merge_convert_script',
        python_callable=merge_convert_script,
        op_kwargs={'script_name': 'merge_convert.slurm', 
                   'dir': masks_dir,
                   'dst': mask_img},
        provide_context=True,
        dag=dag,
    )

    handle_merge_convert = SlurmJobHandlingSensor(
        task_id='handle_merge_convert',
        ssh_conn_id='slurm_ssh_connection_yueyang',
        script_name='merge_convert.slurm',
        remote_path=remote_path,
        local_path=local_path,
        dag=dag,
        timeout=3600,  # max time in seconds for the sensor to run
        poke_interval=30  # interval in seconds between checks
    )

    create_geojson = PythonOperator(
        task_id='create_convert2geojson_script',
        python_callable=convert2geojson_script,
        op_kwargs={'script_name': 'convert2geojson.slurm', 'input': mask_img, 'output': geojson, 'source': dst_img},
        provide_context=True,
        dag=dag,
    )

    handle_convert2geojson = SlurmJobHandlingSensor(
        task_id='handle_convert2geojson',
        ssh_conn_id='slurm_ssh_connection_yueyang',
        script_name='convert2geojson.slurm',
        remote_path=remote_path,
        local_path=local_path,
        dag=dag,
        timeout=3600,  # max time in seconds for the sensor to run
        poke_interval=30  # interval in seconds between checks
    )

    create_gdal_preprocess >> handle_gdal_preprocess >> create_inference >> handle_inference \
        >> create_merge_convert >> handle_merge_convert >> create_geojson >> handle_convert2geojson