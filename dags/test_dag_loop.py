import logging
import sys
import os
from datetime import datetime
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Variable
array_value = [[1], [2], [3], [4], [5], [6], [7], [8], [9], [10], [11], [12], [13], [14], [15]]

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

def return_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def calculate_time_elapsed(**kwargs):

    ti  = kwargs.get('ti')

    xcom_val = ti.xcom_pull(task_ids="start_timer_task")

    start_time = datetime.strptime(xcom_val, '%Y-%m-%d %H:%M:%S')

    end_time = datetime.strptime(return_time(), '%Y-%m-%d %H:%M:%S')

    return end_time - start_time


def plus_10_traditional(*op_args):
    return op_args[0] + 10


@dag(
    dag_id="test_dag_loop",
    start_date=datetime(2024, 4, 1),
    schedule='@daily',
    catchup=False,
    default_args={
        "retries": 2,
    },
    tags=["loop_demo_dag"],
)

def loop_demo_dag():

    start_task = EmptyOperator(
        task_id='start_task'
    )

    start_timer_task = PythonOperator(
        task_id="start_timer_task",
        python_callable=return_time
    )

    end_timer_task = PythonOperator(
        task_id="end_timer_task",
        python_callable=calculate_time_elapsed
    )

    end_task = EmptyOperator(
        task_id='end_task'
    )

    for i in array_value:
        val = i[0]
        plus_10_task = PythonOperator(
            task_id=f"plus_10_task_{str(val)}", 
            python_callable=plus_10_traditional,
            op_args=[val],
        )
        start_timer_task >> plus_10_task >> end_timer_task

    end_timer_task >> end_task

    start_task >> start_timer_task 


loop_demo_dag()