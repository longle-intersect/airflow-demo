import sys
import logging

import datetime
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

logging.basicConfig(stream=sys.stdout, level=logging.INFO)


create_base_tables_query = """
CREATE TABLE IF NOT EXISTS adoption_center_1
(date DATE, type VARCHAR, name VARCHAR, age INTEGER);

CREATE TABLE IF NOT EXISTS adoption_center_2
(date DATE, type VARCHAR, name VARCHAR, age INTEGER);

INSERT INTO
    adoption_center_1 (date, type, name, age)
VALUES
    ('2022-01-01', 'Dog', 'Bingo', 4),
    ('2022-02-02', 'Cat', 'Bob', 7),
    ('2022-03-04', 'Fish', 'Bubbles', 2);

INSERT INTO
    adoption_center_2 (date, type, name, age)
VALUES
    ('2022-06-10', 'Horse', 'Seabiscuit', 4),
    ('2022-07-15', 'Snake', 'Stripes', 8),
    ('2022-08-07', 'Rabbit', 'Hops', 3);
"""

# DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2025, 2, 11),
}


with DAG(
    dag_id="lineage_combine",
    default_args=default_args,
    description='A simple tutorial lineage combine using postgres operator DAG',
    start_date=datetime.datetime(2025, 2, 12),
    schedule_interval="0 0 * * *",
    catchup=False,
) as dag:
    
    create_base_table = PostgresOperator(
        task_id="create_base_table",
        postgres_conn_id='postgres-default',
        sql=create_base_tables_query,
    )
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id='postgres-default',
        sql="""
            CREATE TABLE IF NOT EXISTS animal_adoptions_combined (
        date DATE,
        type VARCHAR,
        name VARCHAR,
        age INTEGER
        );
          """,
    )
    insert_data = PostgresOperator(
        task_id='combine',
        postgres_conn_id='postgres-default',
        sql="""
        INSERT INTO animal_adoptions_combined (date, type, name, age) 
        SELECT * 
        FROM adoption_center_1
        UNION 
        SELECT *
        FROM adoption_center_2;
        """
    ) 
    create_base_table >> create_table >> insert_data