#  Copyright 2021 Collate
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""
You can run this DAG from the default OM installation
"""

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


# DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
}


def explode():
    raise Exception("Oh no!")


with DAG(
    'lineage_tutorial_operator',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 29),
    catchup=False,
    tags=['example'],
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
        hostPort="http://192.168.144.161:30079",
        authProvider="openmetadata",
        securityConfig=OpenMetadataJWTClientConfig(
            jwtToken="eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImluZ2VzdGlvbi1ib3QiLCJyb2xlcyI6WyJJbmdlc3Rpb25Cb3RSb2xlIl0sImVtYWlsIjoiaW5nZXN0aW9uLWJvdEBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3MzY0MjU3NzEsImV4cCI6bnVsbH0.TU_hx_rvqpH4bw6kwbSDWePpFvblqn_H-VjAhCWXOzn5DtlILGyPs15JN2_pYSeMzOAfegj7bBlgB-WBzHjzP4xWvpdbhczoL5ZJtxlEnaGcysFQcGQwOFh87Al4jWGf2fR3vfvKQCIEZDsxOVJYB_jousSXJG6mjS6yasdqmy18e3IyQ7TMEF6GokUVIygbPg2VIGFoQEdmLeBIg6mTUUBX6_L32DAd7kOZIlZarJKGL8kA5hWUrydn5jxN1UDE_mgXgjJdYNbQyuTJqmWLyvUhanpZSGswFPqlltabpHH2PEcUJ4ChMnJwHhSsFrdRTqs3JBnkRCMuI8sDPlkeSg"
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