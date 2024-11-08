from datetime import datetime, timedelta
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task


# Simulating the delay within each function
def simulate_delay(seconds):
    time.sleep(seconds)

# DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2024, 11, 1),
}

@dag(
    dag_id="test_dmt_parallel",
    default_args=default_args,
    description='Test dmt async',    
    schedule=None,
    catchup=True,
    #max_active_runs=1,  # <-- I have tried removing this, and the problem persists. 
)
def image_processing_dag():
    @task
    def get_data_samples():
        return [5, 10, 12]

    @task(multiple_outputs=True)
    def download_image(sample):
        print(f"Downloading {sample}")
        simulate_delay(sample)
        return {"sample": sample, "status": "downloaded"}

    @task(multiple_outputs=True)
    def process_image(data):
        sample = data["sample"]
        print(f"Processing {sample}")
        simulate_delay(sample)
        return {"sample": sample, "status": "processed"}

    @task
    def upload_image(data):
        sample = data["sample"]
        print(f"Uploading {sample}")
        simulate_delay(sample)
        return f"{sample} uploaded"

    samples = get_data_samples()
    downloaded = download_image.expand(samples=samples)
    processed = process_image.expand(data=downloaded)
    uploaded = upload_image.expand(data=processed)

image_processing_flow = image_processing_dag()