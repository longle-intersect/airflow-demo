import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

# DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

@dag(
    dag_id = "ol_counter",
    default_args=default_args,
    schedule='*/1 * * * *',
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    description='DAG that generates a new count value equal to 1.'
)

def counter():

    query1 = PostgresOperator(
        task_id='if_not_exists',
        postgres_conn_id='postgres_default',
        sql='''
        CREATE TABLE IF NOT EXISTS counts (value INTEGER);
        ''',
    )

    query2 = PostgresOperator(
        task_id='inc',
        postgres_conn_id='postgres_default',
        sql='''
        INSERT INTO "counts" (value) VALUES (1);
        ''',
    )

    query1 >> query2

counter()
