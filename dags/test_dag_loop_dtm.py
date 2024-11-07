import logging
import sys
from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

# Function to return the current time
def return_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Function to calculate time elapsed
def calculate_time_elapsed(**kwargs):
    ti = kwargs.get('ti')
    xcom_val = ti.xcom_pull(task_ids="start_timer_task")
    start_time = datetime.strptime(xcom_val, '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime(return_time(), '%Y-%m-%d %H:%M:%S')
    return end_time - start_time

# Function to return a list of numbers for dynamic task mapping
def one_two_three_traditional():
    return [[i] for i in range(1, 16)]

# Function to add 10 to a given number
def plus_10_traditional(x):
    return x[0] + 10  # Adjusted to access first element of list

# Function to subtract 3 from a given number
def minus_3_traditional(x):
    return x - 3

@dag(
    dag_id="test_dag_loop_dmt",
    start_date=datetime(2024, 4, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args={
        "retries": 2,
    },
    tags=["dynamic_task_mapping_demo"],
)
def dynamic_tsk_map_dag():

    start_task = EmptyOperator(
        task_id='start_task'
    )

    start_timer_task = PythonOperator(
        task_id="start_timer_task",
        python_callable=return_time
    )

    one_two_three_task = PythonOperator(
        task_id="one_two_three_task",
        python_callable=one_two_three_traditional
    )

    # Mapping the `plus_10_traditional` function dynamically
    plus_10_task = PythonOperator.partial(
        task_id="plus_10_task",
        python_callable=plus_10_traditional
    ).expand(op_args=one_two_three_task.output)

    # Mapping the `minus_3_traditional` function dynamically
    minus_3_task = PythonOperator.partial(
        task_id="minus_3_task", 
        python_callable=minus_3_traditional
    ).expand(op_args=plus_10_task.output)

    end_timer_task = PythonOperator(
        task_id="end_timer_task",
        python_callable=calculate_time_elapsed
    )

    end_task = EmptyOperator(
        task_id='end_task'
    )

    # Setting the dependencies
    start_task >>\
        start_timer_task >>\
        one_two_three_task >>\
        plus_10_task >>\
        minus_3_task >>\
        end_timer_task >>\
        end_task

dynamic_dag = dynamic_tsk_map_dag()