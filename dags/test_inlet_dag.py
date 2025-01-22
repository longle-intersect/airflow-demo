import json
from pendulum import datetime

from airflow.decorators import (
    dag,
    task,
)

from airflow.datasets import Dataset


@dag(
    dag_id="test_inlet_dag",
    schedule=[Dataset("outlet")],
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "retries": 2,  # If a task fails, it will retry 2 times.
    },
    tags=["example"],
)
def inlet():
    @task()
    def read_xcom(ti=None):
        xcoms = ti.xcom_pull(
            dag_id="test_outlet_dag",
            task_ids="write_xcom",
            key="outlet_xcom",
            include_prior_dates=True,
        )
        print(f"xcoms: {xcoms}")

    read_xcom()


inlet()