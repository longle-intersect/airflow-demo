# example_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
#from plugins.slurm_ssh_operator import SlurmSSHTaskOperator, SlurmJobSensor
from airflow.operators.python import PythonOperator
# slurm_ssh_operator.py
from airflow.models.baseoperator import BaseOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.utils.decorators import apply_defaults
from airflow.sensors.base_sensor_operator import BaseSensorOperator

class SlurmSSHTaskOperator(BaseOperator):
    template_fields = ('script_name', 'remote_path')

    @apply_defaults
    def __init__(
        self,
        ssh_conn_id,
        script_name,
        remote_path,
        *args, **kwargs):
        super(SlurmSSHTaskOperator, self).__init__(*args, **kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.script_name = script_name
        self.remote_path = remote_path

    def execute(self, context):
        local_path = f'/home/airflow/slurm_scripts/{self.script_name}'
        remote_script_path = f'{self.remote_path}/{self.script_name}'
        
        ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        with ssh_hook.get_conn() as ssh_client:
            sftp_client = ssh_client.open_sftp()
            sftp_client.put(local_path, remote_script_path)
            sftp_client.close()

            command = f'sbatch {remote_script_path}'
            stdin, stdout, stderr = ssh_client.exec_command(command)
            result = stdout.read().decode('utf-8').strip()
            error = stderr.read().decode('utf-8').strip()
            if error:
                self.log.error(f"Error in SSH command execution: {error}")
                raise Exception("Error in SSH command execution.")
            self.log.info(f"Command output: {result}")
            job_id = result.split()[-1]  # Assumes Slurm outputs "Submitted batch job <job_id>"
            return job_id

class SlurmJobSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, ssh_conn_id, job_id, *args, **kwargs):
        super(SlurmJobSensor, self).__init__(*args, **kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.job_id = job_id

    def poke(self, context):
        ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        with ssh_hook.get_conn() as ssh_client:
            command = f'squeue -j {self.job_id}'
            stdin, stdout, stderr = ssh_client.exec_command(command)
            result = stdout.read().decode('utf-8').strip()
            if 'PD' in result or 'R' in result:
                self.log.info(f"Job {self.job_id} is still running")
                return False
            self.log.info(f"Job {self.job_id} has completed or does not exist")
            return True


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Python function to create SLURM script
def create_slurm_script(**kwargs):
    script_content = """#!/bin/bash
#SBATCH --job-name=test_airflow_job
#SBATCH --output=./job_output_test.txt
#SBATCH --error=./test_job_error.txt
#SBATCH -n 1
#SBATCH --mem=500M
#SBATCH -t 00:10:00

# Specify the work to be done
sleep 15
echo "Welcome to SDC! I'm Long Le"
"""
    script_path = '/home/airflow/slurm_scripts/slurm_script.slurm'
    with open(script_path, 'w') as file:
        file.write(script_content)
    return script_path

with DAG('slurm_ssh_workflow',
         default_args=default_args,
         description='A simple DAG to submit and monitor Slurm jobs via SSH',
         schedule_interval=None,
         start_date=datetime(2023, 1, 1),
         catchup=False) as dag:

    start = DummyOperator(task_id='start')

    create_script = PythonOperator(
        task_id='create_slurm_script',
        python_callable=create_slurm_script,
        provide_context=True,
        dag=dag,
    )

    submit_slurm_job = SlurmSSHTaskOperator(
        task_id='submit_slurm_job',
        ssh_conn_id='slurm_ssh_connection',
        script_name='slurm_script.slurm',
        remote_path='/home/lelong/job_script'
    )

    monitor_slurm_job = SlurmJobSensor(
        task_id='monitor_slurm_job',
        ssh_conn_id='ssh_hpc_slurm',
        job_id="{{ task_instance.xcom_pull(task_ids='submit_slurm_job') }}"
    )

    end = DummyOperator(task_id='end')

    start >> create_script >> submit_slurm_job >> monitor_slurm_job >> end
