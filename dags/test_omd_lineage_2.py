from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

from metadata.generated.schema.entity.data.container import Container
from metadata.generated.schema.entity.data.table import Table
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=1),
}


with DAG(
    "test-lineage-no-omdingest",
    default_args=default_args,
    description="An example DAG which runs a lineage test",
    start_date=days_ago(1),
    is_paused_upon_creation=False,
    catchup=False,
) as dag:


    t0 = DummyOperator(
        task_id='task0',
        inlets=[
            {"entity": "container", "fqn": "Container A", "key": "group_A"},
            {"entity": "table", "fqn": "Table X", "key": "group_B"},
        ]
    )

    t1 = DummyOperator(
        task_id='task10',
        outlets=[
            {"entity": "table", "fqn": "Table B", "key": "group_A"},
            {"entity": "table", "fqn": "Table Y", "key": "group_B"},
        ]
    )

    t0 >> t1