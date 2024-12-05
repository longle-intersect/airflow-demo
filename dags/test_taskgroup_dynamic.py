from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task, task_group
import pendulum
from datetime import datetime, timedelta
import random

# Define a custom operator as a placeholder, assuming it takes a task_id and mapped_args
from airflow.models.baseoperator import BaseOperator

class MyCustomOperator(BaseOperator):

    def __init__(self, mapped_args, **kwargs) -> None:
        super().__init__(**kwargs)
        self.mapped_args = mapped_args

    def execute(self, context):
        self.cur_value = self.mapped_args.resolve(context)

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
            task1 = MyCustomOperator(task_id='task1', mapped_args=mapped_args)
            task1

        if mapped_args:
            dynamic_task_group_node.expand(mapped_args=mapped_args)

    [start >> task_group_node()]

dag = dynamic_task_group_dag()