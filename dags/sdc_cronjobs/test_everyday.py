# Import the necessary modules from the Airflow library
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define the default arguments for all tasks in the DAG
default_args = {
    'owner': 'airflow',                   # Owner of the DAG
    'depends_on_past': False,             # Does not prevent future runs if a past run fails
    'email': ['your_email@example.com'],  # Replace with your email to receive alerts
    'email_on_failure': True,             # Sends an email if a task fails
    'email_on_retry': False,              # No email on retry
    'retries': 1,                         # Number of retries before considering a task as failed
    'retry_delay': timedelta(minutes=5),  # Time between retries
    'start_date': datetime(2024, 10, 11), # Start date of the DAG
}

# Initialize the DAG with its properties
dag = DAG(
    'test_rs_everyday_tasks',              # Name of the DAG
    default_args=default_args,         # Arguments defined above
    description='Daily tasks for rscovernight',  # Description of the DAG
    schedule_interval='@daily',        # Run once a day at midnight
    catchup=False                      # Do not catch up if a run is missed
)

# Define tasks using BashOperator

# Task to delete old files every day at 17:45
delete_old_files = BashOperator(
    task_id='delete_old_files',
    bash_command='bash rscovernight/cron/delete_old_files.sh',
    dag=dag
)

# Task to update sentinel at 07:00 and 19:00 every day
update_sentinel_morning = BashOperator(
    task_id='update_sentinel_morning',
    bash_command='bash rscovernight/cron/updatesentinel_cron.sh',
    dag=dag
)

update_sentinel_evening = BashOperator(
    task_id='update_sentinel_evening',
    bash_command='bash rscovernight/cron/updatesentinel_cron.sh',
    dag=dag
)

# Task to run vpr pull at 03:00 every day
run_vpr_pull = BashOperator(
    task_id='run_vpr_pull',
    bash_command='bash rscovernight/cron/run_vpr_pull.sh',
    dag=dag
)

# Task to update USGS Landsat at 03:30 every day
update_usgs_landsat = BashOperator(
    task_id='update_usgs_landsat',
    bash_command='bash rscovernight/cron/updateusgslandsat_cron.sh',
    dag=dag
)

# Set task dependencies explicitly
# These tasks do not depend on each other and can run independently
# If there were dependencies, you would set them here using the bitshift operators
# For example:
# task1 >> task2  # task2 runs after task1

# In this setup, all tasks are independent and can run simultaneously