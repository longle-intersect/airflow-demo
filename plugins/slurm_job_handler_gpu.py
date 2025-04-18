from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow.plugins_manager import AirflowPlugin
import time
import os
import sys
import logging
sys.path.insert(0, '/opt/airflow/dags/repo/utils')
sys.path.insert(0, '/opt/airflow/utils')

#local_path = '/home/airflow/slurm_scripts/'
logger = logging.getLogger(__file__)

SLURM_POKE_GRACE_PERIOD_SECONDS = 60
"""The interval where the sensor will not query the queue.
This is useful if in poke mode, where poking slurm or other queue occurs before
the job is registered in the queue.
"""

class SlurmJobHandlingSensorGPU(BaseSensorOperator):
    template_fields = ('script_name', 'remote_path')

    @apply_defaults
    def __init__(self, ssh_conn_id, script_name, remote_path, local_path, *args, **kwargs):
        super(SlurmJobHandlingSensorGPU, self).__init__(*args, **kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.script_name = script_name
        self.remote_path = remote_path
        self.local_path = local_path
        self.job_id = None
        os.makedirs(self.local_path, mode=0o777, exist_ok=True)
    #def execute(self, context):
    #    job_id = self._submit_job()
    #    return job_id

    def poke(self, context):

        if not self.job_id:
            self.job_id = self._submit_job()
            return False
        
        #job_id = context['ti'].xcom_pull(task_ids=self.task_id)
        job_done = self._check_job_status()
        if job_done:
            self.log.info(f"Job {self.job_id} has completed or does not exist")
            output_content, error_content = self._fetch_job_files()
            context['task_instance'].xcom_push(key='output_content', value=output_content)
            context['task_instance'].xcom_push(key='error_content', value=error_content)
            self.log.info(f"Output of {self.job_id}: {output_content}")

            # if error_content:
            #     self.log.error(f"Error for job {self.job_id}: {error_content}")
            #     raise AirflowException('Error in Slurm job execution: Task failed because of upstream errors')
 
            return True
        else:
            self.log.info(f"Job {self.job_id} is still running")
            return False

    def _submit_job(self):
        ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        with ssh_hook.get_conn() as ssh_client:
            remote_script_path = f'{self.remote_path}/{self.script_name}'
            remote_output_path = f'{self.remote_path}/{self.script_name}.out'
            remote_error_path = f'{self.remote_path}/{self.script_name}.err'
            local_script_path = f'{self.local_path}/{self.script_name}'

            sftp_client = ssh_client.open_sftp()
            sftp_client.put(local_script_path, remote_script_path)
            sftp_client.close()

            # command = f'module unload RSstandard'
            # stdin, stdout, stderr = ssh_client.exec_command(command)
            # command = f'module load slurm'
            # stdin, stdout, stderr = ssh_client.exec_command(command)
            # command = f'sbatch --output={remote_output_path} --error={remote_error_path} {remote_script_path}'
            # stdin, stdout, stderr = ssh_client.exec_command(command)
            # run multiple commands
            command = f'module unload RSstandard; module load slurm; sbatch --output={remote_output_path} --error={remote_error_path} {remote_script_path}'
            stdin, stdout, stderr = ssh_client.exec_command(command)
            try:
                job_info = stdout.read().decode('utf-8').strip()
                job_id = job_info.split()[-1]  # Assumes Slurm outputs "Submitted batch job <job_id>"
            except IndexError:
                job_info = stderr.read().decode('utf-8').strip()
                raise AirflowException(f"Error submitting job: {job_info}")
            return job_id

    def _check_job_status(self):
        ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        with ssh_hook.get_conn() as ssh_client:
            command = f'squeue -j {self.job_id}' # -o "%T"
            stdin, stdout, stderr = ssh_client.exec_command(command)
            result = stdout.read().decode('utf-8').strip()
            self.log.info(f"Checking result of Slurm job ID: {result}")

            # Parse the result, expected to have headers on the first call
            lines = result.splitlines()
            if len(lines) > 1:  # First line is headers, subsequent lines are job info
                _, _, _, _, status, _, _, _ = lines[1].split()
                self.log.info(f"Current status of job {self.job_id}: {status}")
                if status in ['R', 'PD']:
                    return False
            else:
                return True

    def _fetch_job_files(self,):
        ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        with ssh_hook.get_conn() as ssh_client:
            sftp_client = ssh_client.open_sftp()
            remote_output_path = f'{self.remote_path}/{self.script_name}.out'
            remote_error_path = f'{self.remote_path}/{self.script_name}.err'
            local_output_path = f'{self.local_path}/{self.script_name}_{self.job_id}.out'
            local_error_path = f'{self.local_path}/{self.script_name}_{self.job_id}.err'
            sftp_client.get(remote_output_path, local_output_path)
            sftp_client.get(remote_error_path, local_error_path)
            sftp_client.close()

            # Read the contents of the downloaded files
            with open(local_output_path, 'r') as file_out:
                output_content = file_out.read()
            with open(local_error_path, 'r') as file_err:
                error_content = file_err.read()

            # TODO: Handle the case jobs are canceled.
            self.log.info(f"Output and error files retrieved: {local_output_path}, {local_error_path}")

            return output_content, error_content

class AirflowJobSchedulerPlugin(AirflowPlugin):
    name = 'slurm_job_handler_basic'
    operators = [SlurmJobHandlingSensorGPU]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprint = []
    menu_links = []