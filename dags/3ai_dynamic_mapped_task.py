from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from datetime import datetime
import pandas as pd
import numpy as np
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2025, 1, 19),
}

@dag(dag_id='dynamic_mapped_task_dag',
     default_args=default_args,
     schedule_interval=None,
     start_date=datetime(2025, 1, 19),
     tags=['dynamic', 'mapped'])
def dynamic_mapped_task_dag():
    
    @task
    def extract_data():
        df = pd.DataFrame({
            'date': pd.date_range(start='2021-01-01', periods=100, freq='D'),
            'sales': np.random.randint(1, 100, size=100),
            'cost': np.random.randint(1, 50, size=100),
            'region': np.random.choice(['North', 'South', 'East', 'West'], size=100)
        })
        # Convert DataFrame to JSON to make it serializable
        return df.to_json(date_format='iso', orient='split')

    @task(multiple_outputs=True)
    def transform_data(region, data):
        # Convert JSON back to DataFrame
        df = pd.read_json(data, orient='split')
        regional_data = df[df['region'] == region]
        regional_data['profit'] = regional_data['sales'] - regional_data['cost']
        # Optionally convert to JSON again if passing to another task
        return regional_data.to_json(date_format='iso', orient='split')

    @task
    def load_data(*args):
        for data_json in args:
            df = pd.read_json(data_json, orient='split')
            print(df.head())

    start = DummyOperator(task_id='Start')
    end = DummyOperator(task_id='End')

    data = extract_data()
    results = transform_data.expand(region=['North', 'South', 'East', 'West'], data=[data])
    load = load_data.expand(args=results)

    start >> data >> results >> load >> end

dag_instance = dynamic_mapped_task_dag()