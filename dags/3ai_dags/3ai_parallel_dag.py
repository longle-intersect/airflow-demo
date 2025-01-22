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
with DAG(dag_id='3ai_parallel_data_pipeline',
         default_args=default_args,
         description='Demo Parallel DAG Pattern',
         schedule_interval=None,
         start_date=days_ago(1),
         tags=['3ai', 'parallel']) as dag:
    
    # Dummy task to mark the start of the pipeline
    start = DummyOperator(task_id='Start')

    # Task to extract data
    def extract_data():
        df = pd.DataFrame({
            'date': pd.date_range(start='2021-01-01', periods=100, freq='D'),
            'sales': np.random.randint(1, 100, size=100),
            'cost': np.random.randint(1, 50, size=100),
            'units': np.random.randint(1, 10, size=100)
        })
        return df

    extract = PythonOperator(
        task_id='Extract_Data',
        python_callable=extract_data
    )

    # Parallel task to transform sales data
    def transform_sales(ti):
        df = ti.xcom_pull(task_ids='Extract_Data')
        df['sales_profit'] = df['sales'] * 0.2  # example calculation
        return df[['date', 'sales_profit']]

    transform_sales = PythonOperator(
        task_id='Transform_Sales',
        python_callable=transform_sales
    )

    # Parallel task to transform cost data
    def transform_cost(ti):
        df = ti.xcom_pull(task_ids='Extract_Data')
        df['cost_overhead'] = df['cost'] + 10  # example overhead addition
        return df[['date', 'cost_overhead']]

    transform_cost = PythonOperator(
        task_id='Transform_Cost',
        python_callable=transform_cost
    )

    # Parallel task to calculate units sold
    def calculate_units(ti):
        df = ti.xcom_pull(task_ids='Extract_Data')
        df['total_units'] = df['units'].cumsum()  # cumulative sum of units
        return df[['date', 'total_units']]

    calculate_units = PythonOperator(
        task_id='Calculate_Units',
        python_callable=calculate_units
    )

    # Task to load data
    def load_data(ti):
        sales_data = ti.xcom_pull(task_ids='Transform_Sales')
        cost_data = ti.xcom_pull(task_ids='Transform_Cost')
        units_data = ti.xcom_pull(task_ids='Calculate_Units')

        # Combine data for demonstration; here we assume all dataframes have the same index
        combined_data = pd.concat([sales_data.set_index('date'), cost_data.set_index('date'), units_data.set_index('date')], axis=1)
        print(combined_data.head())

    load = PythonOperator(
        task_id='Load_Data',
        python_callable=load_data
    )

    # Dummy task to mark the end of the pipeline
    end = DummyOperator(task_id='End')

    # Set task dependencies to create a parallel flow
    start >> extract >> [transform_sales, transform_cost, calculate_units] >> load >> end