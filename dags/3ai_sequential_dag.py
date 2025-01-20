from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import pandas as pd
import numpy as np

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2025, 1, 19),
}

# Define the DAG
with DAG(dag_id='3ai_sequential_data_pipeline',
        default_args=default_args,
        description='Demo Sequential DAG Pattern',
        schedule_interval=None,
        start_date=days_ago(1),
        tags=['3ai', 'demo']) as dag:
    
    # Dummy task to mark the start of the pipeline
    start = DummyOperator(task_id='Start')

    # Task to extract data
    def extract_data():
        # Create a DataFrame with synthetic data
        df = pd.DataFrame({
            'date': pd.date_range(start='2021-01-01', periods=100, freq='D'),
            'sales': np.random.randint(1, 100, size=100),
            'cost': np.random.randint(1, 50, size=100)
        })
        return df

    extract = PythonOperator(
        task_id='Extract_Data',
        python_callable=extract_data
    )

    # Task to transform data
    def transform_data(ti):
        # Get the data from the previous task
        df = ti.xcom_pull(task_ids='Extract_Data')
        # Example transformation: calculate profit
        df['profit'] = df['sales'] - df['cost']
        return df

    transform = PythonOperator(
        task_id='Transform_Data',
        python_callable=transform_data
    )

    # Task to load data
    def load_data(ti):
        # Get the transformed data from the previous task
        transformed_df = ti.xcom_pull(task_ids='Transform_Data')
        # Print the data to the logs for demonstration purposes
        print(transformed_df.head())

    load = PythonOperator(
        task_id='Load_Data',
        python_callable=load_data
    )

    # Dummy task to mark the end of the pipeline
    end = DummyOperator(task_id='End')

    # Set task dependencies to create a sequential flow
    start >> extract >> transform >> load >> end