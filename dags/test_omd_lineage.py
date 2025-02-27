from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from metadata.generated.schema.entity.services.databaseService import DatabaseService
from metadata.generated.schema.entity.data.database import Database
from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema
from metadata.generated.schema.entity.data.table import Table
from metadata.generated.schema.api.services.createDatabaseService import CreateDatabaseServiceRequest
from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest
from metadata.generated.schema.api.data.createDatabaseSchema import CreateDatabaseSchemaRequest
from metadata.generated.schema.api.data.createTable import CreateTableRequest
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity
from metadata.ingestion.ometa.ometa_api import OpenMetadata

from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
)
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
    OpenMetadataJWTClientConfig,
)

from metadata.generated.schema.entity.data.table import (
    Column,
)

from airflow_provider_openmetadata.hooks.openmetadata import OpenMetadataHook

# OpenMetadata connection configuration
# metadata_config = {
#     "server_config": {
#         "hostPort": "http://172.19.0.4:8585/api",
#         "authProvider": "openmetadata",
#         "securityConfig": OpenMetadataJWTClientConfig(
#             #jwtToken="eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhZG1pbiIsImlzQm90IjpmYWxzZSwiaXNzIjoib3Blbi1tZXRhZGF0YS5vcmciLCJpYXQiOjE2NjM5Mzg0NjIsImVtYWlsIjoiYWRtaW5Ab3Blbm1ldGFkYXRhLm9yZyJ9.tS8um_5DKu7HgzGBzS1VTA5uUjKWOCU0B_j08WXBiEC0mr0zNREkqVfwFDD-d24HlNEbrqioLsBuFRiwIWKc1m_ZlVQbG7P36RUxhuv2vbSp80FKyNM-Tj93FDzq91jsyNmsQhyNv_fNr3TXfzzSPjHt8Go0FMMP66weoKMgW2PbXlhVKwEuXUHyakLLzewm9UMeQaEiRzhiTMU3UkLXcKbYEJJvfNFcLwSl9W8JCO_l0Yj3ud-qt_nQYEZwqW6u5nfdQllN133iikV4fM5QZsMCnm8Rq1mvLR0y9bmJiD7fwM1tmJ791TUWqmKaTnP49U493VanKpUAfzIiOiIbhg"
#             jwtToken="eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImluZ2VzdGlvbi1ib3QiLCJyb2xlcyI6WyJJbmdlc3Rpb25Cb3RSb2xlIl0sImVtYWlsIjoiaW5nZXN0aW9uLWJvdEBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3MzY1MDk2MjIsImV4cCI6bnVsbH0.BsXWKMCSWpqzOwBAGO0ID1fKQqsWWdCOwB1qBvIDx7nHx9p5rQkH8pdLwAQ4k-3WFJhCftsXwvTo52SS6BxFpd-CfW6xN3uKJUR22PmyWEAieTFaS1JIekBXQMCN7_FQbHN29btVNohLnOoVLgVoR7rFUNM8hKVRceaXU5z0PVPDJvUl8JMIFNN8X8YM2Jn1896ZOyQFa9TYxc7q7yLkV0ZBR6LPhnnHkJNSih4Kq3oSc4C-8pi-DOgE4s2xGagv8dx3uFrDqV_fwlj41jhmwoRHyibXITxZTRf5GOmru89_YXrFUPKWl8S1hsCf38DXunl6-Mt1_HSGyeqvIlYtvg"
#             #jwtToken="eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImxpbmVhZ2UtYm90Iiwicm9sZXMiOlsiTGluZWFnZUJvdFJvbGUiXSwiZW1haWwiOiJsaW5lYWdlLWJvdEBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3MzY1MDk2MjIsImV4cCI6bnVsbH0.y-wvbfAxGK_bnKP19UhVY9v2Fymv0K7m5ZqV2nn_ThHtZy6B4u8NBY8MO09XP7w_NHqogK3KPrjulWaPzH7UwsmM6FGiQ368Ti-GXN-2oTWBEHF-1YW2-x3mueqZdTj9rN6CQjZzF1i6f92FxG1isXfDZLV_ePxdjEXyvJUUxkW-Vz0sUNZcKONmUBejQ4rzJlDoH6aFXA1oTg0VYD6HDDp_F9IATl0J5q-5uwCSKslfmL5qwGgSSHkBPrgjj-8ECwGZYRq-oegYl6523HQ8yM5gMyAPVgz42L1WPXTgGxl274S5h5J_vE-mR_z36WAi_OZsO8Ci6jrr9W78Yc74dA"
#         ),
#     }
# }

server_config = OpenMetadataConnection(
    hostPort="http://172.19.0.4:8585/api",
    authProvider="openmetadata",
    securityConfig=OpenMetadataJWTClientConfig(
        #jwtToken="eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhZG1pbiIsImlzQm90IjpmYWxzZSwiaXNzIjoib3Blbi1tZXRhZGF0YS5vcmciLCJpYXQiOjE2NjM5Mzg0NjIsImVtYWlsIjoiYWRtaW5Ab3Blbm1ldGFkYXRhLm9yZyJ9.tS8um_5DKu7HgzGBzS1VTA5uUjKWOCU0B_j08WXBiEC0mr0zNREkqVfwFDD-d24HlNEbrqioLsBuFRiwIWKc1m_ZlVQbG7P36RUxhuv2vbSp80FKyNM-Tj93FDzq91jsyNmsQhyNv_fNr3TXfzzSPjHt8Go0FMMP66weoKMgW2PbXlhVKwEuXUHyakLLzewm9UMeQaEiRzhiTMU3UkLXcKbYEJJvfNFcLwSl9W8JCO_l0Yj3ud-qt_nQYEZwqW6u5nfdQllN133iikV4fM5QZsMCnm8Rq1mvLR0y9bmJiD7fwM1tmJ791TUWqmKaTnP49U493VanKpUAfzIiOiIbhg"
        jwtToken="eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImluZ2VzdGlvbi1ib3QiLCJyb2xlcyI6WyJJbmdlc3Rpb25Cb3RSb2xlIl0sImVtYWlsIjoiaW5nZXN0aW9uLWJvdEBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3MzY1MDk2MjIsImV4cCI6bnVsbH0.BsXWKMCSWpqzOwBAGO0ID1fKQqsWWdCOwB1qBvIDx7nHx9p5rQkH8pdLwAQ4k-3WFJhCftsXwvTo52SS6BxFpd-CfW6xN3uKJUR22PmyWEAieTFaS1JIekBXQMCN7_FQbHN29btVNohLnOoVLgVoR7rFUNM8hKVRceaXU5z0PVPDJvUl8JMIFNN8X8YM2Jn1896ZOyQFa9TYxc7q7yLkV0ZBR6LPhnnHkJNSih4Kq3oSc4C-8pi-DOgE4s2xGagv8dx3uFrDqV_fwlj41jhmwoRHyibXITxZTRf5GOmru89_YXrFUPKWl8S1hsCf38DXunl6-Mt1_HSGyeqvIlYtvg"
        #jwtToken="eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImxpbmVhZ2UtYm90Iiwicm9sZXMiOlsiTGluZWFnZUJvdFJvbGUiXSwiZW1haWwiOiJsaW5lYWdlLWJvdEBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3MzY1MDk2MjIsImV4cCI6bnVsbH0.y-wvbfAxGK_bnKP19UhVY9v2Fymv0K7m5ZqV2nn_ThHtZy6B4u8NBY8MO09XP7w_NHqogK3KPrjulWaPzH7UwsmM6FGiQ368Ti-GXN-2oTWBEHF-1YW2-x3mueqZdTj9rN6CQjZzF1i6f92FxG1isXfDZLV_ePxdjEXyvJUUxkW-Vz0sUNZcKONmUBejQ4rzJlDoH6aFXA1oTg0VYD6HDDp_F9IATl0J5q-5uwCSKslfmL5qwGgSSHkBPrgjj-8ECwGZYRq-oegYl6523HQ8yM5gMyAPVgz42L1WPXTgGxl274S5h5J_vE-mR_z36WAi_OZsO8Ci6jrr9W78Yc74dA"
    ),
)


metadata = OpenMetadata(server_config)

# Define Service, Database, Schema, and Table names
SERVICE_NAME = "airflow_dummy_service"  # A dummy service for testing
DATABASE_NAME = f"{SERVICE_NAME}.airflow_dummy_db"
SCHEMA_NAME = "default_schema"
TABLE_X_FQN = f"{DATABASE_NAME}.{SCHEMA_NAME}.TableX"
TABLE_B_FQN = f"{DATABASE_NAME}.{SCHEMA_NAME}.TableB"
TABLE_Y_FQN = f"{DATABASE_NAME}.{SCHEMA_NAME}.TableY"

# Function to create a Service if it does not exist
def ensure_service_exists():
    service = metadata.get_by_name(entity=DatabaseService, fqn=SERVICE_NAME)
    if not service:
        print(f"Service '{SERVICE_NAME}' not found. Creating it...")
        service_request = CreateDatabaseServiceRequest(
            name=SERVICE_NAME,
            serviceType="Postgres"  # Can be any database type (PostgreSQL, Snowflake, etc.)
        )
        metadata.create_or_update(service_request)
        print(f"Service '{SERVICE_NAME}' created.")

# Function to create a Database if it does not exist
def ensure_database_exists():
    database = metadata.get_by_name(entity=Database, fqn=DATABASE_NAME)
    if not database:
        print(f"Database '{DATABASE_NAME}' not found. Creating it...")
        database_request = CreateDatabaseRequest(
            name=DATABASE_NAME,
            service=SERVICE_NAME
        )
        metadata.create_or_update(database_request)
        print(f"Database '{DATABASE_NAME}' created.")

# Function to create a Schema if it does not exist
def ensure_schema_exists():
    schema = metadata.get_by_name(entity=DatabaseSchema, fqn=f"{DATABASE_NAME}.{SCHEMA_NAME}")
    if not schema:
        print(f"Schema '{SCHEMA_NAME}' not found. Creating it...")
        schema_request = CreateDatabaseSchemaRequest(
            name=SCHEMA_NAME,
            database=DATABASE_NAME
        )
        metadata.create_or_update(schema_request)
        print(f"Schema '{SCHEMA_NAME}' created.")

# Function to create a Table if it does not exist
def ensure_table_exists(table_fqn):
    schema = metadata.get_by_name(entity=DatabaseSchema, fqn=f"{DATABASE_NAME}.{SCHEMA_NAME}")
    table = metadata.get_by_name(entity=Table, fqn=table_fqn)
    if not table:
        print(f"Table '{table_fqn}' not found. Creating it...")
        table_request = CreateTableRequest(
            name=table_fqn.split(".")[-1],  # Extract table name
            #fullyQualifiedName=table_fqn,
            databaseSchema=schema.fullyQualifiedName,
            columns= [
                Column(
                    name='type',
                    dataType='STRING',
                    description='Test columne',
                )
            ]
            #schema=SCHEMA_NAME
        )
        metadata.create_or_update(table_request)
        print(f"Table '{table_fqn}' created.")

# Ensure all entities exist
ensure_service_exists()
ensure_database_exists()
ensure_schema_exists()
ensure_table_exists(TABLE_X_FQN)
ensure_table_exists(TABLE_B_FQN)
ensure_table_exists(TABLE_Y_FQN)

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=1),
}

# Define DAG
with DAG(
    "test-lineage",
    default_args=default_args,
    description="An example DAG which registers missing service, database, schema, and tables in OpenMetadata and tracks lineage",
    start_date=days_ago(1),
    is_paused_upon_creation=False,
    catchup=False,
) as dag:

    t0 = DummyOperator(
        task_id='task0',
        inlets=[
            OMEntity(entity=Table, fqn=TABLE_X_FQN, key="group_B"),
        ]
    )

    t1 = DummyOperator(
        task_id='task10',
        outlets=[
            OMEntity(entity=Table, fqn=TABLE_B_FQN, key="group_A"),
            OMEntity(entity=Table, fqn=TABLE_Y_FQN, key="group_B"),
        ]
    )

    t0 >> t1