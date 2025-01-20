from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.decorators import task, dag
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

@dag(dag_id='3ai_dynamic_mapped_task_dag',
     default_args=default_args,
     schedule_interval=None,
     start_date=datetime(2025, 1, 19),
     tags=['3ai','dynamic', 'mapped'])
def dynamic_mapped_task_dag():
    
    @task
    def extract_data():
        # Create a DataFrame with synthetic data
        df = pd.DataFrame({
            'date': pd.date_range(start='2021-01-01', periods=100, freq='D'),
            'sales': np.random.randint(1, 100, size=100),
            'cost': np.random.randint(1, 50, size=100),
            'region': np.random.choice(['North', 'South', 'East', 'West'], size=100)
        })
        return df

    @task(multiple_outputs=True)
    def transform_data(region, data):
        regional_data = data[data['region'] == region]
        regional_data['profit'] = regional_data['sales'] - regional_data['cost']
        print(f"Processed {region} region")
        return regional_data

    @task
    def load_data(*args):
        for df in args:
            print(df.head())

    start = DummyOperator(task_id='Start')
    end = DummyOperator(task_id='End')

    data = extract_data()

    # Map the transform_data task to each region
    results = transform_data.expand(region=['North', 'South', 'East', 'West'], data=[data])

    load = load_data.expand(args=results)

    start >> data >> results >> load >> end

dag_instance = dynamic_mapped_task_dag()