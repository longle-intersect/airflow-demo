"""
You can run this DAG from the default OM installation
"""
import sys
import logging

from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
from airflow_provider_openmetadata.lineage.operator import OpenMetadataLineageOperator

from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
)
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
    OpenMetadataJWTClientConfig,
)

from airflow_provider_openmetadata.hooks.openmetadata import OpenMetadataHook

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
remote_path='/home/lelong/log_airflow_slurm/scripts/'
local_path='/home/airflow/slurm_scripts/' 

# openmetadata_hook = OpenMetadataHook(openmetadata_conn_id="omd_connection")
# server_config = openmetadata_hook.get_conn()

# DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2025, 2, 11),
}


def explode():
    raise Exception("Oh no!")


with DAG(
    'test_lineage_connector',
    default_args=default_args,
    description='A simple test lineage DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 2, 12),
    catchup=False,
    tags=['lineage', 'example'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
        outlets={
            "tables": ["sample_data.ecommerce_db.shopify.dim_address"]
        }
    )

    t2 = BashOperator(
        task_id='sleep',
        depends_on_past=False,
        bash_command='sleep 1',
        retries=3,
        inlets={
            "tables": ["sample_data.ecommerce_db.shopify.dim_customer"]
        }
    )

    risen = PythonOperator(
        task_id='explode',
        provide_context=True,
        python_callable=explode,
        retries=0,
    )

    dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG
    dag.doc_md = """
    This is a documentation placed anywhere
    """  # otherwise, type it like this
    templated_command = dedent("")

    t3 = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command,
        params={'my_param': 'Parameter I passed in'},
    )

    t1 >> [t2, t3]

    server_config = OpenMetadataConnection(
        hostPort="http://172.19.0.4:8585/api",
        authProvider="openmetadata",
        securityConfig=OpenMetadataJWTClientConfig(
            #jwtToken="eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhZG1pbiIsImlzQm90IjpmYWxzZSwiaXNzIjoib3Blbi1tZXRhZGF0YS5vcmciLCJpYXQiOjE2NjM5Mzg0NjIsImVtYWlsIjoiYWRtaW5Ab3Blbm1ldGFkYXRhLm9yZyJ9.tS8um_5DKu7HgzGBzS1VTA5uUjKWOCU0B_j08WXBiEC0mr0zNREkqVfwFDD-d24HlNEbrqioLsBuFRiwIWKc1m_ZlVQbG7P36RUxhuv2vbSp80FKyNM-Tj93FDzq91jsyNmsQhyNv_fNr3TXfzzSPjHt8Go0FMMP66weoKMgW2PbXlhVKwEuXUHyakLLzewm9UMeQaEiRzhiTMU3UkLXcKbYEJJvfNFcLwSl9W8JCO_l0Yj3ud-qt_nQYEZwqW6u5nfdQllN133iikV4fM5QZsMCnm8Rq1mvLR0y9bmJiD7fwM1tmJ791TUWqmKaTnP49U493VanKpUAfzIiOiIbhg"
            jwtToken="eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImluZ2VzdGlvbi1ib3QiLCJyb2xlcyI6WyJJbmdlc3Rpb25Cb3RSb2xlIl0sImVtYWlsIjoiaW5nZXN0aW9uLWJvdEBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3MzY1MDk2MjIsImV4cCI6bnVsbH0.BsXWKMCSWpqzOwBAGO0ID1fKQqsWWdCOwB1qBvIDx7nHx9p5rQkH8pdLwAQ4k-3WFJhCftsXwvTo52SS6BxFpd-CfW6xN3uKJUR22PmyWEAieTFaS1JIekBXQMCN7_FQbHN29btVNohLnOoVLgVoR7rFUNM8hKVRceaXU5z0PVPDJvUl8JMIFNN8X8YM2Jn1896ZOyQFa9TYxc7q7yLkV0ZBR6LPhnnHkJNSih4Kq3oSc4C-8pi-DOgE4s2xGagv8dx3uFrDqV_fwlj41jhmwoRHyibXITxZTRf5GOmru89_YXrFUPKWl8S1hsCf38DXunl6-Mt1_HSGyeqvIlYtvg"
            #jwtToken="eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImxpbmVhZ2UtYm90Iiwicm9sZXMiOlsiTGluZWFnZUJvdFJvbGUiXSwiZW1haWwiOiJsaW5lYWdlLWJvdEBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3MzY1MDk2MjIsImV4cCI6bnVsbH0.y-wvbfAxGK_bnKP19UhVY9v2Fymv0K7m5ZqV2nn_ThHtZy6B4u8NBY8MO09XP7w_NHqogK3KPrjulWaPzH7UwsmM6FGiQ368Ti-GXN-2oTWBEHF-1YW2-x3mueqZdTj9rN6CQjZzF1i6f92FxG1isXfDZLV_ePxdjEXyvJUUxkW-Vz0sUNZcKONmUBejQ4rzJlDoH6aFXA1oTg0VYD6HDDp_F9IATl0J5q-5uwCSKslfmL5qwGgSSHkBPrgjj-8ECwGZYRq-oegYl6523HQ8yM5gMyAPVgz42L1WPXTgGxl274S5h5J_vE-mR_z36WAi_OZsO8Ci6jrr9W78Yc74dA"
        ),
    )

    t4 = OpenMetadataLineageOperator(
        task_id='lineage_op',
        depends_on_past=False,
        server_config=server_config,
        service_name="airflow_lineage_op_service",
        only_keep_dag_lineage=True,
    )

    t1 >> t4