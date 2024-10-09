"""Test flow for job scheduler plugin.
"""

import os
import glob
import shutil
import logging
import datetime as dt
import pendulum

from airflow import DAG
from job_scheduler_plugin import JobSchedulerOperator
from airflow.operators.dummy_operator import DummyOperator

import common.airflow_variables as cv


logger = logging.getLogger(__file__)

#
# Default task behavior
#
START_DATE          = pendulum.datetime(2018,1,1)
DEFAULT_TIMEOUT     = dt.timedelta(minutes=10)
DEFAULT_NRETRIES    = 0
DEFAULT_RETRY_DELAY = dt.timedelta(minutes=1)
DEFAULT_SCHEDULE    = '0 1 * * *'


class TestJobSchedulerFlow:


    def __init__(self, owner, admin_emails,
        mems_home, mems_deps_home,
        dag_id,
        schedule_str=DEFAULT_SCHEDULE,
        timeout=DEFAULT_TIMEOUT, start_date=START_DATE,
        nretries=DEFAULT_NRETRIES, retry_delay=DEFAULT_RETRY_DELAY):
        """

        Parameters
        ----------
        owner : str
            The Airflow user that curates the flow.
        admin_emails : list of str
            The list of emails of whoever is in chanrge of the flow.
        mems_home : str
            The home/base directory of MEMS in the data centre.
        script_path : str
            The full path to the script to be run by the job scheduler
        """

        self.owner = owner
        self.start_date = start_date
        self.nretries = nretries
        self.retry_delay = retry_delay
        self.timeout = timeout

        self.admin_emails = admin_emails
        self.mems_home_path = mems_home
        self.apps_home_path = mems_deps_home
        self.dag_id = dag_id
        self.schedule_str = schedule_str

        airflow_logs = os.path.join(os.environ['AIRFLOW_HOME'], 'logs')
        self.env = {
                'MEMS_HOME': self.mems_home_path,
                'APPS_HOME': self.apps_home_path,
                'LOG_FILE_PREFIX': "{{{{ '/'.join(['{airflow_logs}', "
                                   " ti.dag_id|string, ti.task_id|string, "
                                   " ti.execution_date|string, "
                                   " ti.try_number|string]) }}}}" \
                                        .format(airflow_logs=airflow_logs)
            }

        self.the_dag = None


    def _init_dag(self):
        default_args = {
            'owner': self.owner,
            'depends_on_past': False,
            'start_date': self.start_date,
            'email': self.admin_emails,
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': self.nretries,
            'retry_delay': self.retry_delay,
            'catchup_by_default': False,
            'provide_context': True
        }

        print(self.dag_id)
        self.the_dag = DAG(self.dag_id, catchup=False,
                max_active_runs=4, schedule_interval=self.schedule_str,
                default_args=default_args)

        return self.the_dag


    def _trigger(self, tid):
        trigger =  DummyOperator(task_id=tid, dag=self.the_dag)

        return trigger


    def _run_job_success(self, tid, prev_task):
        """
        Test successful job completion
        """

        script_path = os.path.join(os.environ['AIRFLOW_HOME'], 'scripts',
                                   'job_scheduler_test', 'slurm_test_success.sh')

        run_job = JobSchedulerOperator(
                task_id=tid,
                dag=self.the_dag,
                env=self.env,
                script_with_args=script_path
            )

        if prev_task is not None:
            prev_task >> run_job

        return run_job


    def _run_job_indirect(self, tid, prev_task):
        """
        Test successful job completion using indirect submission

        In this test the submit script does the following:
            > module load slurm
            > sbatch --export=ALL <job_script_path>
        """

        submit_script_path = os.path.join(os.environ['AIRFLOW_HOME'], 'scripts',
                                   'job_scheduler_test', 'slurm_test_indirect.sh')
        job_script_path = os.path.join(os.environ['AIRFLOW_HOME'], 'scripts',
                                          'job_scheduler_test', 'slurm_test_success.sh')
        script_with_args = '{} {}'.format(submit_script_path, job_script_path)

        run_job = JobSchedulerOperator(
                task_id=tid,
                dag=self.the_dag,
                env=self.env,
                script_with_args=script_with_args,
                direct_submit=False
            )

        if prev_task is not None:
            prev_task >> run_job

        return run_job


    def _run_job_failure(self, tid, prev_task):
        """
        Test failing job
        """

        script_path = os.path.join(os.environ['AIRFLOW_HOME'], 'scripts',
                                   'job_scheduler_test', 'slurm_test_failure.sh')

        run_job = JobSchedulerOperator(
                task_id=tid,
                dag=self.the_dag,
                env=self.env,
                script_with_args=script_path
            )

        if prev_task is not None:
            prev_task >> run_job

        return run_job


    def _the_end(self, tid, prev_tasks):
        the_end = DummyOperator(
                task_id=tid,
                dag=self.the_dag
            )

        if prev_tasks is not None and len(prev_tasks) > 0:
            for task in prev_tasks:
                task >> the_end

        return the_end


    def create(self):

        dag = self._init_dag()

        t01 = self._trigger('trigger')
        t02 = self._run_job_success('run_job_success',
                                     prev_task=t01)
        t03 = self._run_job_indirect('run_job_indirect',
                                     prev_task=t02)
        t04 = self._run_job_failure('run_job_failure',
                                     prev_task=t03)
        t05 = self._the_end('end_task', [t04])

        return dag


    @classmethod
    def main(clazz):


        flow = TestJobSchedulerFlow(
            owner          = cv.getvar(cv.TEST_FLOW_OWNER),
            admin_emails   = cv.getvar(cv.TEST_FLOW_ADMIN_EMAILS,
                                       deserialize=True),
            mems_home      = cv.getvar(cv.IRS_MEMS_HOME_PATH),
            mems_deps_home = cv.getvar(cv.IRS_MEMS_APPS_HOME_PATH),
            dag_id         = 'test_job_scheduler_flow',
        )

        return flow.create()

dag = None
if __name__ == '__main__':
    print('tests here...')
else:
    the_dag = TestJobSchedulerFlow.main()