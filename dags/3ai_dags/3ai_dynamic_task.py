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
with DAG(dag_id='3ai_dynamic_task_generation_dag',
         default_args=default_args,
         description='DAG with Dynamic Task Generation',
         schedule_interval=None,
         start_date=days_ago(1),
         tags=['3ai', 'dynamic', 'data']) as dag:
    
    # Dummy task to mark the start of the pipeline
    start = DummyOperator(task_id='Start')

    # Task to extract data
    def extract_data():
        # Create a DataFrame with synthetic data
        df = pd.DataFrame({
            'date': pd.date_range(start='2021-01-01', periods=100, freq='D'),
            'sales': np.random.randint(1, 100, size=100),
            'cost': np.random.randint(1, 50, size=100),
            'region': np.random.choice(['North', 'South', 'East', 'West'], size=100)
        })
        return df

    extract = PythonOperator(
        task_id='Extract_Data',
        python_callable=extract_data
    )

    # Function to generate dynamic tasks
    def generate_dynamic_tasks():
        regions = ['North', 'South', 'East', 'West']
        transform_tasks = []

        for region in regions:
            def transform_data_regionally(ti, region=region):  # Default argument to capture the loop variable
                df = ti.xcom_pull(task_ids='Extract_Data')
                regional_data = df[df['region'] == region]
                profit_data = regional_data.copy()
                profit_data['profit'] = profit_data['sales'] - profit_data['cost']
                print(f"Processed {region} region")
                return profit_data

            task = PythonOperator(
                task_id=f'Transform_Data_{region}',
                python_callable=transform_data_regionally,
                provide_context=True,
                dag=dag
            )
            transform_tasks.append(task)
        
        return transform_tasks

    transform_tasks = generate_dynamic_tasks()

    # Task to load data
    def load_data(ti):
        for task in transform_tasks:
            transformed_df = ti.xcom_pull(task_ids=task.task_id)
            print(transformed_df.head())

    load = PythonOperator(
        task_id='Load_Data',
        python_callable=load_data,
        provide_context=True
    )

    # Dummy task to mark the end of the pipeline
    end = DummyOperator(task_id='End')

    # Set task dependencies to create a sequential flow
    start >> extract >> transform_tasks >> load >> end