from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2025, 1, 19),
}

with DAG(dag_id='3ai_task_group_dag',
         default_args=default_args,
         description='DAG with TaskGroups for data processing',
         schedule_interval=None,
         start_date=datetime(2025, 1, 19),
         tags=['3ai', 'taskgroup', 'data']) as dag:
    
    start = DummyOperator(task_id='Start')
    end = DummyOperator(task_id='End')

    # TaskGroup for data processing
    with TaskGroup(group_id='data_processing') as data_processing:
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
            df = ti.xcom_pull(task_ids=f'{data_processing.group_id}.Extract_Data')
            df['profit'] = df['sales'] - df['cost']
            return df

        transform = PythonOperator(
            task_id='Transform_Data',
            python_callable=transform_data
        )

        # Task to load data
        def load_data(ti):
            transformed_df = ti.xcom_pull(task_ids=f'{data_processing.group_id}.Transform_Data')
            print(transformed_df.head())

        load = PythonOperator(
            task_id='Load_Data',
            python_callable=load_data
        )

        extract >> transform >> load

    start >> data_processing >> end