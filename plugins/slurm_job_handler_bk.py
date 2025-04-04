from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
import time
import os
#local_path = '/home/airflow/slurm_scripts/'

class SlurmJobHandlingSensor(BaseSensorOperator):
    template_fields = ('script_name', 'remote_path')

    @apply_defaults
    def __init__(self, ssh_conn_id, script_name, remote_path, local_path, stage_script, *args, **kwargs):
        super(SlurmJobHandlingSensor, self).__init__(*args, **kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.script_name = script_name
        self.remote_path = remote_path
        self.local_path = local_path
        self.stage_script = stage_script
        self.job_id = None
        self.date = kwargs['date']
        self.processing_stage = kwargs['stage']

    #def execute(self, context):
    #    job_id = self._submit_job()
    #    return job_id

    def _create_slurm_script(self):
        script_content = f"""#!/bin/bash
#SBATCH --job-name={self.script_name}
#SBATCH --output=/home/lelong/log_airflow_slurm/stdout/{self.script_name}.log
#SBATCH --error=/home/lelong/log_airflow_slurm/stderr/{self.script_name}.error
#SBATCH -n 1
#SBATCH --mem=8192M
#SBATCH -t 01:00:00

# Load modules and specify the work
module load sdc_testing
if [ $? -ne 0 ]; then
    echo "Failed to load sdc_testing module."
    exit 1
fi

# Load necessary modules
module load cloud fractionalcover
if [ $? -ne 0 ]; then
    echo "Failed to load cloud fractionalcover module."
    exit 1
fi

# Specify the work to be done
cd $FILESTORE_PATH/tmp_data/
{self.stage_script}
"""
        #{self.stage_script}
        script_path = os.path.join(self.local_path, self.script_name)
        with open(script_path, 'w') as file:
            file.write(script_content)
        return script_path
    
    def poke(self, context):

        if not self.job_id:
            self.script_path = self._create_slurm_script()
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

            command = f'sbatch --output={remote_output_path} --error={remote_error_path} {remote_script_path}'
            stdin, stdout, stderr = ssh_client.exec_command(command)
            job_info = stdout.read().decode('utf-8').strip()
            job_id = job_info.split()[-1]  # Assumes Slurm outputs "Submitted batch job <job_id>"
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

            self.log.info(f"Output and error files retrieved: {local_output_path}, {local_error_path}")

            return output_content, error_content
