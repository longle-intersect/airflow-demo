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
    def __init__(self, ssh_conn_id, task_ids, job_id, *args, **kwargs):
        super(SlurmJobSensor, self).__init__(*args, **kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.task_ids = task_ids
        self.job_id = job_id

    def poke(self, context):
        # Pull job ID from XCom
        job_id = context['task_instance'].xcom_pull(task_ids=self.task_ids)
        if not job_id:
            self.log.info("Job ID not found in XCom.")
            return False

        self.log.info(f"Checking status of Slurm job ID: {job_id}")

        ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        with ssh_hook.get_conn() as ssh_client:
            command = f'squeue -j {job_id}'
            stdin, stdout, stderr = ssh_client.exec_command(command)
            result = stdout.read().decode('utf-8').strip()

            #self.log.info(f"Checking stdin of Slurm job ID: {stdin}")
            #self.log.info(f"Checking error of Slurm job ID: {stderr}")
            self.log.info(f"Checking result of Slurm job ID: {result}")

            # Parse the result, expected to have headers on the first call
            lines = result.splitlines()
            if len(lines) > 1:  # First line is headers, subsequent lines are job info
                _, _, _, _, status, _, _, _ = lines[1].split()
                self.log.info(f"Current status of job {job_id}: {status}")
                if status in ['R', 'PD']:
                    self.log.info(f"Job {job_id} is still running")
                    return False
            else:
                self.log.info(f"Job {job_id} has completed or does not exist")
                return True
