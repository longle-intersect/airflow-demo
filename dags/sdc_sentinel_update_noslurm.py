from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago

# DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'hpc_sentinel_processing',
    default_args=default_args,
    description='Process Sentinel Images on HPC via SSH',
    schedule_interval=None,  # Set to None for manual triggering
    start_date=days_ago(1),
    tags=['hpc', 'sentinel', 'image_processing'],
) as dag:

    # Combine all commands into one large script
    combined_script = """
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

# Execute cloud fmask processing
qv_sentinel2cloud_fmask.py --toaref10 $FILESTORE_PATH/tmp_data/cemsre_t55hdv_20241008_ab0m5.img --updatedatabase
if [ $? -ne 0 ]; then
    echo "Failed at stage 1: Cloud fmask processing."
    exit 1
fi
"""

    # Task 4: Cloud fmask processing
    cloud_fmask_processing = SSHOperator(
        task_id='cloud_fmask_processing',
        ssh_conn_id='ssh_hpc_slurm',
        command=combined_script,
    )

    # Task 5: Topo masks processing
    topo_masks_processing = SSHOperator(
        task_id='topo_masks_processing',
        ssh_conn_id='ssh_hpc_slurm',
        command='qv_sentinel2topomasks.py --toaref10 cemsre_t55hdv_20241008_ab0m5.img',
    )

    # Task 6: Surface reflectance processing
    surface_reflectance_processing = SSHOperator(
        task_id='surface_reflectance_processing',
        ssh_conn_id='ssh_hpc_slurm',
        command='doSfcRefSentinel2.py --toaref cemsre_t55hdv_20241008_ab0m5.img',
    )

    # Task 7: Water index processing
    water_index_processing = SSHOperator(
        task_id='water_index_processing',
        ssh_conn_id='ssh_hpc_slurm',
        command='qv_water_index2015.py cemsre_t55hdv_20241008_abam5.img cemsre_t55hdv_20241008_abbm5.img --omitothermasks',
    )

    # Task 8: Fractional cover processing
    fractional_cover_processing = SSHOperator(
        task_id='fractional_cover_processing',
        ssh_conn_id='ssh_hpc_slurm',
        command='qv_fractionalcover_sentinel2.py cemsre_t55hdv_20241008_abam5.img',
    )

    # Task Dependency Setup
    load_testing_module >> recall_files >> load_cloud_module
    load_cloud_module >> cloud_fmask_processing >> topo_masks_processing >> surface_reflectance_processing
    surface_reflectance_processing >> water_index_processing >> fractional_cover_processing

