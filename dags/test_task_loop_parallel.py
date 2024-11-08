import datetime as dt
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

def get_data_samples():
    # Simulated list of image identifiers
    return [5, 10, 12]

def download_image(sample):
    print(f"Downloading {sample}")
    time.sleep(sample)  # Simulate a 5-second download time
    return f"{sample}_downloaded"

def process_image(sample):
    print(f"Processing {sample}")
    time.sleep(sample)  # Simulate a 10-second processing time
    return f"{sample}_processed"

def upload_image(sample):
    print(f"Uploading {sample}")
    time.sleep(sample)  # Simulate a 5-second upload time
    return f"{sample}_uploaded"

with DAG(
    dag_id="test_continuous_image_processing",
    schedule_interval=None,  # No automatic triggering
    start_date=dt.datetime(2024, 11, 1),  # Adjust start date as needed
    tags=["dynamic", "image_processing", "parallel"]
) as dag:

    get_samples = PythonOperator(
        task_id='get_data_samples',
        python_callable=get_data_samples,
        do_xcom_push=True
    )

    # Dynamically create tasks for each sample
    for i, sample in enumerate(get_data_samples()):
        download = PythonOperator(
            task_id=f'download_image_{i}',
            python_callable=download_image,
            op_args=[sample]
        )

        process = PythonOperator(
            task_id=f'process_image_{i}',
            python_callable=process_image,
            op_args=[sample]
        )

        upload = PythonOperator(
            task_id=f'upload_image_{i}',
            python_callable=upload_image,
            op_args=[sample]
        )

        # Set the task sequence for each sample
        get_samples >> download >> process >> upload