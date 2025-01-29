import datetime
import pendulum

from airflow.lineage import AUTO
from airflow.lineage.entities import File
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

FILE_CATEGORIES = ["CAT1", "CAT2", "CAT3"]

dag = DAG(
    dag_id="example_lineage",
    start_date=pendulum.datetime(2025, 1, 28, tz="UTC"),
    schedule="0 0 * * *",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)

f_final = File(url="/tmp/final")
run_this_last = EmptyOperator(task_id="run_this_last", dag=dag, inlets=AUTO, outlets=f_final)

f_in = File(url="/tmp/whole_directory/")
outlets = []
for file in FILE_CATEGORIES:
    f_out = File(url="/tmp/{}/{{{{ data_interval_start }}}}".format(file))
    outlets.append(f_out)

run_this = BashOperator(task_id="run_me_first", bash_command="echo 1", dag=dag, inlets=f_in, outlets=outlets)
run_this.set_downstream(run_this_last)