import logging
import sys
import os
from datetime import datetime
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

def return_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def calculate_time_elapsed(**kwargs):

    ti  = kwargs.get('ti')
    xcom_val = ti.xcom_pull(task_ids="start_timer_task")
    start_time = datetime.strptime(xcom_val, '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime(return_time(), '%Y-%m-%d %H:%M:%S')

    return end_time - start_time

def one_two_three_traditional():
    # This is the array that is used to meausre the run times between the 2 approaches
    return [[1], [2], [3], [4], [5], [6], [7], [8], [9], [10], [11], [12], [13], [14], [15]]

def plus_10_traditional(x):
    # Since x is a list containing a single integer, we access it and then return a new list
    return [x[0] + 10]

def minus_3_traditional(x):
    # Adjust the function to handle a list containing a single integer
    return [x[0] - 3]

@dag(
    dag_id="test_dag_loop_dmt",
    start_date=datetime(2024, 4, 1),
    schedule='@daily',
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

    plus_10_task = PythonOperator.partial(
        task_id="plus_10_task", python_callable=plus_10_traditional
    ).expand(op_args=one_two_three_task.output)

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

    start_task >>\
        start_timer_task >>\
        one_two_three_task >>\
        plus_10_task >>\
        minus_3_task >>\
        end_timer_task >>\
        end_task


dynamic_tsk_map_dag()