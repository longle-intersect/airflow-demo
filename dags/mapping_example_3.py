from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import random

@dag(dag_id="test_batch_procesisng", schedule_interval=None, start_date=days_ago(1), catchup=False)
def sophisticated_batch_processing_dag():
    @task
    def list_datasets():
        # Simulate listing datasets by returning a list of dataset identifiers
        num_datasets = random.randint(1, 9)  # Random number less than 10
        return [f"dataset{i}" for i in range(1, num_datasets + 1)]

    @task
    def preprocess_dataset(dataset_id):
        # Simulate preprocessing steps
        print(f"Preprocessing {dataset_id}")
        # Return a new identifier for the preprocessed dataset
        return f"preprocessed_{dataset_id}"

    @task
    def perform_calculations(preprocessed_id):
        # Simulate some calculations
        print(f"Calculating on {preprocessed_id}")
        # Return some made-up results
        return random.random()

    @task
    def aggregate_results(results):
        # Aggregate results
        avg_result = sum(results) / len(results)
        print(f"Average Result: {avg_result}")

    # Generate dynamic tasks based on datasets
    datasets = list_datasets()
    preprocessed_data = preprocess_dataset.expand(dataset_id=datasets)
    calculation_results = perform_calculations.expand(preprocessed_id=preprocessed_data)
    aggregate_results(calculation_results)

example_dag = sophisticated_batch_processing_dag()