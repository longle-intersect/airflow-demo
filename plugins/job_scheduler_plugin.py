#Copyright 2016 The Office of Environment and Heritage NSW, Australia all rights reserved,
#Nic Hannah <nic@doubleprecision.com.au>
#Permission to use, modify, and distribute this software is given under the
#terms of the Modular Emissions Modelling System (MEMS, BSD style) license.
#See LICENSE.txt that came with this distribution for specifics.
#
#NO WARRANTY IS EXPRESSED OR IMPLIED.  USE AT YOUR OWN RISK.
#
#


import os
import time
import subprocess as sp
from datetime import timedelta
import re
import logging

from abc import ABC, abstractmethod

from airflow.exceptions import AirflowException, AirflowRescheduleException
from airflow.models import TaskReschedule, Variable
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin

from airflow.utils import timezone


logger = logging.getLogger(__file__)

SLURM_POKE_GRACE_PERIOD_SECONDS = 60
"""The interval where the sensor will not query the queue.
This is useful if in poke mode, where poking slurm or other queue occurs before
the job is registered in the queue.
"""



class JobSchedulerOperator(BaseSensorOperator):

    template_fields = ('script_with_args', 'env')

    @apply_defaults
    def __init__(self, script_with_args, env=None, log_path=None,
                 direct_submit=True, job_scheduler_name='slurm', do_module_load=True,
                 *args, **kwargs):
        """
        This operator plugin submits a job scheduler job and periodically (and
        asynchronously) checks whether the job has completed or failed.
        If a failure of any kind occurs (such as a timeout) then the job is
        removed from the queue.

        The log output of the job is appended to the airflow log.

        Parameters
        ----------
        script_with_args : str
            The path to a script to run, can include command line arguments.
        env : dict
            Environment variables made available to the job.
        log_path :
            Path to log file to be used by the job script. If set to None then
            it will default to:

                $AIRFLOW_HOME/<dag_id>/<task_id>/<execution_date>/
                            <try_number>_<job_scheduler_name>.out

            If this exists at the end of the run then it's contents will be
            written to the Airflow log.

            Please read below for the behaviour of log_path when
            direct_submit=False is being used.

        direct_submit : bool
            If True the script_with_args parameter is a job script and will
            be submitted directly to the job scheduler - e.g. by calling
            'sbatch' or similar.

            If False the script_with_args parameter is a regular bash script
            that will itself make the submission. In this case the last line of
            output from the script must be the output of the submission command
            - this will be used by the operator to check the job status.

            Note that if False then the operator has no control over where logs
            are written because this may be set within the bash script
            mentioned above. In this case it is important to set the <log_path>
            argument correctly.

            A common use-case for direct_script=False would be to 'module load'
            the job scheduler software before submitting the job script.

            Defaut is direct_submit=True
        job_scheduler_name : str
            The name of the job scheduler to use. Only 'slurm' is upported.

        do_module_load : bool
            Run 'module load <job_scheduler_name>' before running the job scheduler.
        """

        kwargs['mode'] = 'reschedule'

        super(JobSchedulerOperator, self).__init__(*args, **kwargs)
        self.script_with_args = script_with_args
        self.env = env
        self.log_path = log_path
        self.direct_submit = direct_submit
        self.job_scheduler_name = job_scheduler_name

        assert job_scheduler_name == 'slurm'
        module_load_str = None
        if do_module_load:
            module_load_str = 'source {}/init/bash && module load {}' \
                                    .format(os.environ['MODULESHOME'],
                                            job_scheduler_name)
        self.job_scheduler = Slurm(module_load_str=module_load_str)


    def poke(self, context):
        """
        Check state of job

        Returns
        -------
        bool
            Has the job completed successfully or not
        """

        job_id = self.__get_job_id(context)
        state, exit_code = self.job_scheduler.status(job_id)

        logger.debug('poked job id %s, returned: state=%s, exit_code=%s',
                job_id, state, exit_code)
        
        if state == JobSheduler.STATE_UNKNOWN:
            return False

        if exit_code:
            raise AirflowException('Scheduled job failed')

        return (state == JobSheduler.STATE_COMPLETED)


    def execute(self, context):
        """
        Run operator, submits batch job and relies on super to periodically
        check status.
        """

        if self.log_path is None:
            ti = context['ti']
            filename = '{0}_{1}.out'.format(ti.try_number,
                                            self.job_scheduler_name)
            path = os.path.join(os.environ['AIRFLOW_HOME'], 'logs',
                                ti.dag_id, ti.task_id,
                                str(ti.execution_date))
            self.log_path = os.path.join(path, filename)

            logger.debug('log_path: %s.', self.log_path)

        task_reschedules = TaskReschedule.find_for_task_instance(context['ti'])
        logger.debug('task_reschedules is "%s"', task_reschedules)

        if not task_reschedules:
            logger.debug('No task reschedule - doing submit.')
            # If this is the first call to execute then submit job
            job_id = self.job_scheduler.submit(self.script_with_args,
                                               self.env, self.log_path,
                                               self.direct_submit)
            self.__set_job_id(context, job_id)
            time.sleep(SLURM_POKE_GRACE_PERIOD_SECONDS)

        try:
            super().execute(context)
        except Exception as e:
            if type(e) != AirflowRescheduleException:
                self.__finalise_job(context)
            raise e

        # The above will raise a AirflowRescheduleException if rescheduling a
        # poke. The following will only be run when poke returns True,
        # meaning that the job has completed successfully so we call
        # finalise to clean up.
        self.__finalise_job(context)


    def __finalise_job(self, context):
        """
        Called when the task instance completes (retry, failure or success).
        Cleans up any pending job.
        """

        job_id = self.__get_job_id(context)

        # cancel job - this will return without error if the job is already
        # complete
        self.job_scheduler.cancel(job_id)
        self.__clear_job_id(context)

        # Open log path and write to airflow log
        assert self.log_path is not None, 'self.log_path not set'
        if os.path.exists(self.log_path):
            with open(self.log_path) as f:
                for line in f:
                    logger.info('{0}: {1}' \
                            .format(self.job_scheduler_name.upper(),
                                    line.rstrip()))


    @staticmethod
    def __make_job_id_key(context):
        """
        Construct a unique key which will be used to track a job id
        """

        ti_key = '_'.join(map(str, context['ti'].key))
        return 'JOB_SCHEDULER_JOB_ID_KEY-{}'.format(ti_key)


    @staticmethod
    def __set_job_id(context, job_id):
        """
        Store the slurm job id using a key specific to this task instance
        """

        key = JobSchedulerOperator.__make_job_id_key(context)
        return Variable.set(key, job_id)


    @staticmethod
    def __get_job_id(context):
        """
        Get slurm job id for this task instance
        """

        key = JobSchedulerOperator.__make_job_id_key(context)
        return Variable.get(key)


    @staticmethod
    def __clear_job_id(context):
        """
        Clean up global variable used to track slurm job id
        """

        key = JobSchedulerOperator.__make_job_id_key(context)
        Variable.delete(key)


    """
    Alternative implementation of the above two functions. This does not work
    because when a task is rescheduled Xcom is cleared. e.g. see:
    https://stackoverflow.com/questions/57759423/persisting-state-on-airflow-sensor-rescheduled-runs

    @staticmethod
    def set_external_resource_id(context, value):
        return context['ti'].xcom_push(key=JOB_SCHEULDER_JOB_ID_KEY
                                       value=value)

    @staticmethod
    def get_external_resource_id(context):
        return context['ti'].xcom_pull(task_ids=context['task'].task_id,
                                       key=JOB_SCHEDULER_JOB_ID_KEY)
    """


class JobSheduler(ABC):

    STATE_UNKNOWN = 'STATE_UNKNOWN'
    STATE_RUNNING = 'STATE_RUNNING'
    STATE_PENDING = 'STATE_PENDING'
    STATE_COMPLETED = 'STATE_COMPLETED'
    STATE_FAILED = 'STATE_FAILED'

    @abstractmethod
    def submit(self, script_with_args, env):
        pass

    @abstractmethod
    def cancel(self, job_id):
        pass

    @abstractmethod
    def status(self, job_id):
        pass



class Slurm(JobSheduler):

    status_map = {'COMPLETED' : JobSheduler.STATE_COMPLETED,
                  'RUNNING' : JobSheduler.STATE_RUNNING,
                  'PENDING' : JobSheduler.STATE_PENDING,
                  'FAILED' : JobSheduler.STATE_FAILED}

    def __init__(self, module_load_str=None):

        if module_load_str is not None:
            self.sbatch_cmd = '{} && sbatch'.format(module_load_str)
            self.sacct_cmd = '{} && sacct'.format(module_load_str)
            self.scancel_cmd = '{} && scancel'.format(module_load_str)
            self.use_shell = True
        else:
            self.sbatch_cmd = 'sbatch'
            self.sacct_cmd = 'sacct'
            self.scancel_cmd = 'scancel'
            self.use_shell = False


    def submit(self, script_with_args, env, log_path=None, direct_submit=True):
        """
        Submit a job to the job scheduler

        Parameters
        ----------
        script_with_args : str
            The path to a script to run, can include command line arguments.
        env : dict
            Environment variables made available to the job
        log_path :
            Path to log file to be used by the job script. If set to None then
            it will default to:

                $AIRFLOW_HOME/<dag_id>/<task_id>/<execution_date>/
                            <try_number>_<job_scheduler_name>.out

            If this exists at the end of the run then it's contents will be
            written to the Airflow log.

            Please read below for the behaviour of log_path when
            direct_submit=False is being used.

        direct_submit : bool
            If True the script_with_args parameter is a job script and will
            be submitted directly to the job scheduler - e.g. by calling
            'sbatch' or similar.

            If False the script_with_args parameter is a regular bash script
            that will itself make the submission. In this case the last line of
            output from the script must be the output of the submission command
            - this will be used by the operator to check the job status.

            Note that if False then the operator has no control over where logs
            are written because this may be set within the bash script
            mentioned above. In this case it is important to set the <log_path>
            argument correctly.

            A common use-case for direct_script=False would be to 'module load'
            the job scheduler software before submitting the job script.

            Defaut is direct_submit=True

        Returns
        -------
        str
            The job id
        """

        def parse_sbatch_output(output):
            # Only look at the last line of multi-line output
            output = output.strip().split('\n')

            m = re.match('Submitted batch job (\d+)', output[-1])
            if m is None:
                logger.error("sbatch output can't be parsed: {}".format(output))
                raise AirflowException("Could not parse slurm output. "
                                       "Please ensure that script returns "
                                       "a string with last line: "
                                       "'Submitted batch job <job_id>'")
            return m.group(1)

        def env_to_cmd_line_arg(env_dict):
            """
            Slurm does not do a good job of using environment variables from
            the submission environment. This function creates a command line
            argument to do this.
            """
            cmd_arg = '--export='

            first = True
            for k, v in env_dict.items():
                if first:
                    cmd_arg += '{}={}'.format(k, v)
                    first = False
                else:
                    cmd_arg += ',{}={}'.format(k, v)

            return cmd_arg

        if direct_submit:
            cmd_str = ' '.join([self.sbatch_cmd, env_to_cmd_line_arg(env),
                                '--output={0}'.format(log_path), script_with_args])
        else:
            cmd_str = script_with_args

        logger.info('running script with command {}'.format(cmd_str))
        logger.info('running script with env {}'.format(env))

        if self.use_shell:
            output = sp.check_output(cmd_str, env={**env, **os.environ}, shell=True)
        else:
            output = sp.check_output(cmd_str.split(), env={**env, **os.environ})
        job_id = parse_sbatch_output(output.decode('utf-8'))

        logger.info('submitted job, got job_id: {}'.format(job_id))

        return job_id


    def cancel(self, job_id):
        """
        Cancel job

        Parameters
        ----------
        job_id : str
            The job id to cancel
        """

        cmd_str = ' '.join([self.scancel_cmd, job_id])
        if self.use_shell:
            sp.check_output(cmd_str, shell=True)
        else:
            sp.check_output(cmd_str.split())

        logger.info('cancelled job with job_id: {}'.format(job_id))


    def status(self, job_id):
        """
        Get status of job

        Parameters
        ----------
        job_id : str
            The job id to cancel

        Returns
        -------
        str
            A status string
        int
            Return code of the job, only valid if job is complete
        """

        def parse_sacct_output(output):
            if output == '':
                state = JobSheduler.STATE_UNKNOWN
                exit_code = None
                logger.warning('job_id {} is in {}'.format(job_id, state))
            else:
                fields = output.split('|')

                state = Slurm.status_map[fields[1]]
                exit_code = int(fields[2].split(':')[0])
                logger.info('job_id {} is in {}'.format(job_id, state))

            return state, exit_code

        cmd_str = ' '.join([self.sacct_cmd, '-X', '-p', '-n', '-b', '-j', job_id])

        if self.use_shell:
            print(f'df-status 3: {cmd_str}')
            output = sp.check_output(cmd_str, shell=True)
        else:
            print(f'df-status 4: {cmd_str}')
            output = sp.check_output(cmd_str.split())

        print(f'df-status 5: {output}')
        output = output.decode('utf-8')

        return parse_sacct_output(output)


class AirflowJobSchedulerPlugin(AirflowPlugin):
    name = 'job_scheduler_plugin'
    operators = [JobSchedulerOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprint = []
    menu_links = []
