from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task, task_group
import pendulum
from datetime import datetime, timedelta
import random

# Define a custom operator as a placeholder, assuming it takes a task_id and mapped_args
class MyCustomOperator(PythonOperator):
    def __init__(self, **kwargs):
        super().__init__(
            python_callable=lambda: print("Processing", kwargs['mapped_args']),
            **kwargs
        )

def set_mapped_args():
    # Simulate output that would be mapped to multiple tasks
    start = 1
    end = random.randint(3, 8)
    dummy_mapped_args = list(range(start, end + 1))
    #dummy_mapped_args = [1, 2, 3, 4, 5]
    Variable.set("output_of_start", dummy_mapped_args, serialize_json=True)

# DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2024, 12, 1),
}

@dag(
    dag_id="dynamic_task_group_dag",
    default_args=default_args,
    description='Test task group with different batch',
    schedule_interval= None, #"0 0 * * *",  # Runs daily at midnight
    catchup=False,
    start_date=datetime(2024, 12, 1, tzinfo=pendulum.timezone('Australia/Sydney'))
)
def dynamic_task_group_dag():
    start = PythonOperator(
        task_id="start",
        python_callable=set_mapped_args,
    )

    @task_group(group_id="task_group")
    def task_group_node():
        mapped_args = Variable.get("output_of_start", default_var=[], deserialize_json=True)

        @task_group(group_id="dynamic_task_group")
        def dynamic_task_group_node(mapped_args):
            # Create a custom task for each mapped argument
            for arg in mapped_args:
                task = MyCustomOperator(
                    task_id=f'task_{arg}',
                    mapped_args=arg
                )
                task

        # Expand the dynamic task group only if mapped_args is not empty
        if mapped_args:
            dynamic_task_group_node(mapped_args=mapped_args)

    # Link the start task to the task group
    start >> task_group_node()

# Instantiate the DAG
dag = dynamic_task_group_dag()