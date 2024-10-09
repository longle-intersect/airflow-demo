from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
import time

class SlurmJobHandlingSensor(BaseSensorOperator):
    template_fields = ('script_name', 'remote_path')

    @apply_defaults
    def __init__(self, ssh_conn_id, script_name, remote_path, *args, **kwargs):
        super(SlurmJobHandlingSensor, self).__init__(*args, **kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.script_name = script_name
        self.remote_path = remote_path

    def execute(self, context):
        job_id = self._submit_job()
        return job_id

    def poke(self, context):
        job_id = context['ti'].xcom_pull(task_ids=self.task_id)
        job_done = self._check_job_status(job_id)
        if job_done:
            self._fetch_job_files(job_id)
        return job_done

    def _submit_job(self):
        ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        with ssh_hook.get_conn() as ssh_client:
            remote_script_path = f'{self.remote_path}/{self.script_name}'
            remote_output_path = f'{self.remote_path}/{self.script_name}.out'
            remote_error_path = f'{self.remote_path}/{self.script_name}.err'
            local_script_path = f'/home/airflow/slurm_scripts/{self.script_name}'

            sftp_client = ssh_client.open_sftp()
            sftp_client.put(local_script_path, remote_script_path)
            sftp_client.close()

            command = f'sbatch --output={remote_output_path} --error={remote_error_path} {remote_script_path}'
            stdin, stdout, stderr = ssh_client.exec_command(command)
            job_info = stdout.read().decode('utf-8').strip()
            job_id = job_info.split()[-1]  # Assumes Slurm outputs "Submitted batch job <job_id>"
            return job_id

    def _check_job_status(self, job_id):
        ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        with ssh_hook.get_conn() as ssh_client:
            command = f'squeue -j {job_id} -o "%T"'
            stdin, stdout, stderr = ssh_client.exec_command(command)
            result = stdout.read().decode('utf-8').strip()
            if 'COMPLETED' in result or 'FAILED' in result or 'CANCELLED' in result:
                return True
            return False

    def _fetch_job_files(self, job_id):
        ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        with ssh_hook.get_conn() as ssh_client:
            sftp_client = ssh_client.open_sftp()
            remote_output_path = f'{self.remote_path}/{self.script_name}.out'
            remote_error_path = f'{self.remote_path}/{self.script_name}.err'
            local_output_path = f'/tmp/{self.script_name}_{job_id}.out'
            local_error_path = f'/tmp/{self.script_name}_{job_id}.err'
            sftp_client.get(remote_output_path, local_output_path)
            sftp_client.get(remote_error_path, local_error_path)
            sftp_client.close()
            self.log.info(f"Output and error files retrieved: {local_output_path}, {local_error_path}")
