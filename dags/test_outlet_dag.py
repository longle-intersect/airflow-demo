from pendulum import datetime

from airflow.decorators import (
    dag,
    task,
)

from airflow.datasets import Dataset


@dag(
    dag_id="test_outlet_dag",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "retries": 2,  # If a task fails, it will retry 2 times.
    },
    tags=["example"],
)  # If set, this tag is shown in the DAG view of the Airflow UI
def outlet():
    @task(outlets=[Dataset("outlet")])
    def write_xcom(ti=None):
        ti.xcom_push(key="outlet_xcom", value="xyz")

    write_xcom()


outlet()