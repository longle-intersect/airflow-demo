from datetime import datetime
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.models.param import Param
from airflow.decorators import task

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG, its scheduling, and set catchup to False
with DAG(
    dag_id="test_secure_dtn_access",
    default_args=default_args,
    description="A simple DAG to test secure SSH connection and submit a SLURM job",
    schedule_interval= None, #'*/5 * * * *',
    start_date=datetime(2025, 3, 6),
    catchup=False,
    tags=["ssh", "example"],
    params={
        "key_file": Param(
            " ",
            type="string",
            description="Path to the SSH key file."
        ),
        "command": Param(
            'echo "Run command successfully!"',
            type="string",
            escription="Command to run in DTN Host."
        )
    },
) as dag:

    @task(task_id="test_secure_ssh_connection")
    def test_ssh_connection(**kwargs) -> str:
        params: ParamsDict = kwargs["params"]
        print(f"Keyfile is {params["key_file"]}")
        ssh_hook = SSHHook(ssh_conn_id='secure_ssh_conn', key_file=params["key_file"])
        ssh_op = SSHOperator(
            task_id='ssh_test',
            ssh_hook=ssh_hook,
            command='echo "SSH connection successful!"',
            do_xcom_push=True  # Ensures the output is pushed to XCom
        )
        result = ssh_op.execute(None)
        kwargs["ti"].xcom_push(key="ssh_test_output", value=result)  # Manually push to XCom if needed
        return "SSH connection tested successfully."

    @task(task_id="submit_download_job")
    def download_job(**kwargs) -> str:
        params: ParamsDict = kwargs["params"]
        ssh_hook = SSHHook(ssh_conn_id='secure_ssh_conn', key_file=params["key_file"])
        ssh_op = SSHOperator(
            task_id='ssh_download_job',
            ssh_hook=ssh_hook,
            command=params["command"],
            do_xcom_push=True
        )
        result = ssh_op.execute(None)
        #kwargs["ti"].xcom_push(key="download_job_result", value=result)
        return "SLURM job submitted successfully."

    test_connection = test_ssh_connection()
    download_job_dtn = download_job()

    test_connection >> download_job_dtn
