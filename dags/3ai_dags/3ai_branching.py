from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
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
with DAG(dag_id='3ai_branching_data_pipeline',
         default_args=default_args,
         description='Demo Branching DAG Pattern',
         schedule_interval=None,
         start_date=days_ago(1),
         tags=['3ai', 'branching']) as dag:
    
    # Dummy task to mark the start of the pipeline
    start = DummyOperator(task_id='Start')

    # Task to extract data
    def extract_data():
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
        df = ti.xcom_pull(task_ids='Extract_Data')
        df['profit'] = df['sales'] - df['cost']
        return df

    transform = PythonOperator(
        task_id='Transform_Data',
        python_callable=transform_data
    )

    # Branching decision based on profit
    def decide_what_to_do_next(ti):
        df = ti.xcom_pull(task_ids='Transform_Data')
        average_profit = df['profit'].mean()
        if average_profit > 20:
            return 'high_profit_task'
        else:
            return 'low_profit_task'

    branch_task = BranchPythonOperator(
        task_id='Branching',
        python_callable=decide_what_to_do_next
    )

    # Dummy tasks for each scenario
    high_profit_task = DummyOperator(task_id='high_profit_task')
    low_profit_task = DummyOperator(task_id='low_profit_task')

    # Task to load data
    def load_data(ti):
        transformed_df = ti.xcom_pull(task_ids='Transform_Data')
        print(transformed_df.head())

    load = PythonOperator(
        task_id='Load_Data',
        python_callable=load_data
    )

    # Dummy task to mark the end of the pipeline
    end = DummyOperator(task_id='End')

    # Set task dependencies with branching logic
    start >> extract >> transform >> branch_task >> [high_profit_task, low_profit_task] >> load >> end