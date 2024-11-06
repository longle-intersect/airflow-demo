from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

@dag(dag_id="test_dynamic_mapping_2", schedule_interval=None, start_date=days_ago(1), catchup=False)
def example_dynamic_mapping_dag():

    @task
    def list_files():
        # Example function that returns a list of file names
        return ["file1.csv", "file2.csv", "file3.csv"]

    @task
    def process_file(file_name):
        # Process each file
        print(f"Processing {file_name}")

    file_names = list_files()
    process_file.expand(file_name=file_names)

example_dag = example_dynamic_mapping_dag()