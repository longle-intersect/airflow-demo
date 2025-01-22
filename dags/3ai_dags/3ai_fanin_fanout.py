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
with DAG(dag_id='3ai_fan_out_fan_in_pipeline',
         default_args=default_args,
         description='Demo Fan-out/Fan-in DAG Pattern',
         schedule_interval=None,
         start_date=days_ago(1),
         tags=['3ai', 'fan-out-fan-in']) as dag:
    
    # Dummy task to mark the start of the pipeline
    start = DummyOperator(task_id='Start')

    # Task to generate data
    def generate_data():
        df = pd.DataFrame({
            'date': pd.date_range(start='2021-01-01', periods=100, freq='D'),
            'value': np.random.rand(100)
        })
        return df

    generate_data_task = PythonOperator(
        task_id='Generate_Data',
        python_callable=generate_data
    )

    # Fan-out: Multiple independent tasks processing parts of the data
    def process_data_part(index, **kwargs):
        ti = kwargs['ti']
        df = ti.xcom_pull(task_ids='Generate_Data')
        processed_data = df.iloc[index*20:(index+1)*20]  # Split data into chunks
        processed_data['processed_value'] = processed_data['value'] * 2  # Simple processing
        return processed_data

    process_tasks = [PythonOperator(
        task_id=f'Process_Part_{i}',
        python_callable=process_data_part,
        op_kwargs={'index': i}
    ) for i in range(5)]  # Creates 5 tasks to process parts of the data

    # Fan-in: Task to aggregate results
    def aggregate_data(ti):
        results = [ti.xcom_pull(task_ids=f'Process_Part_{i}') for i in range(5)]
        aggregated_df = pd.concat(results)
        print(aggregated_df.head())
        return aggregated_df

    aggregate_data_task = PythonOperator(
        task_id='Aggregate_Data',
        python_callable=aggregate_data
    )

    # Dummy task to mark the end of the pipeline
    end = DummyOperator(task_id='End')

    # Set task dependencies
    start >> generate_data_task >> process_tasks >> aggregate_data_task >> end