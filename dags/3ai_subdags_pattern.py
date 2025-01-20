from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import pandas as pd
import numpy as np

def create_subdag(parent_dag_id, child_dag_id, args):
    """
    Create a subDAG for processing a specific part of data.
    """
    subdag = DAG(
        dag_id=f'{parent_dag_id}.{child_dag_id}',
        default_args=args,
        schedule_interval="@daily",
    )

    with subdag:
        start = DummyOperator(
            task_id='start'
        )

        def process_data():
            # Simulate some data processing
            print("Processing data in subDAG")
            return pd.DataFrame({
                'data': np.random.randint(1, 100, size=10)
            })

        process_task = PythonOperator(
            task_id='process_data',
            python_callable=process_data
        )

        end = DummyOperator(
            task_id='end'
        )

        start >> process_task >> end

    return subdag

# Main DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2025, 1, 19),
}

dag_id = '3ai_main_dag_with_subdags'
main_dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    description='Demo DAG with SubDAGs Pattern',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['3ai', 'subdags']
)

start_main = DummyOperator(
    task_id='Start_main',
    dag=main_dag
)

# Define subDAGs
subdag1 = SubDagOperator(
    task_id='subdag1',
    subdag=create_subdag(dag_id, 'subdag1', default_args),
    dag=main_dag,
)

subdag2 = SubDagOperator(
    task_id='subdag2',
    subdag=create_subdag(dag_id, 'subdag2', default_args),
    dag=main_dag,
)

end_main = DummyOperator(
    task_id='End_main',
    dag=main_dag
)

# Setting up the dependencies
start_main >> subdag1 >> subdag2 >> end_main