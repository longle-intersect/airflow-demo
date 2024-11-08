import time
import pendulum
from airflow.decorators import dag, task


@dag(
    dag_id="test_trigger_one_success",
    schedule=None,
    start_date=pendulum.datetime(2024, 11, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,  # <-- I have tried removing this, and the problem persists. 
)
def etl_sleep():

    @task
    def get_symbols():
        res = [('A', 1, 111), ('B', 2, 222), ('C', 3, 333)]
        return res

    @task
    def extract(symbol_info, data_interval_end=None):
        # Do some work...
        time.sleep(symbol_info[1])
        return symbol_info

    @task(trigger_rule="one_success")
    def transform(symbol_info, data_interval_end=None):
        # Do some work...
        return symbol_info

    @task(trigger_rule="one_success")
    def load(symbol_info, data_interval_end=None):
        # Do some work...
        return symbol_info

    # DAG
    symbols = get_symbols()
    raw_symbols_data = extract.expand(symbol_info=symbols)
    clean_symbols_data = transform.expand(symbol_info=raw_symbols_data)
    load.expand(symbol_info=clean_symbols_data)


etl_sleep()