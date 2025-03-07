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
DATABASE_NAME = f"{SERVICE_NAME}.stac-fastapi"
SCHEMA_NAME = "SDC_Sentinel_Collection_5"

# TABLE_X_FQN = f"{DATABASE_NAME}.{SCHEMA_NAME}.TableX"
# TABLE_B_FQN = f"{DATABASE_NAME}.{SCHEMA_NAME}.TableB"
# TABLE_Y_FQN = f"{DATABASE_NAME}.{SCHEMA_NAME}.TableY"

TABLES = {
    "ab0": f"{DATABASE_NAME}.{SCHEMA_NAME}.cemsre_t55hdv_20241008_ab0m5",  # Raw Sentinel-2 input
    "ab0_zdem": f"{DATABASE_NAME}.{SCHEMA_NAME}.cemsre_t55hdv_20241008_ab0m5_zdem",  # Raw Sentinel-2 input
    "ab1": f"{DATABASE_NAME}.{SCHEMA_NAME}.cemsre_t55hdv_20241008_ab1m5",  # Additional input
    "ab1_zdem": f"{DATABASE_NAME}.{SCHEMA_NAME}.cemsre_t55hdv_20241008_ab1m5_zdem",  # Raw Sentinel-2 input
    "ab2": f"{DATABASE_NAME}.{SCHEMA_NAME}.cemsre_t55hdv_20241008_ab2m5",  # Additional input

    "fmaskcloud_ad2": f"{DATABASE_NAME}.{SCHEMA_NAME}.cemsre_t55hdv_20241008_ad2m5",  # Cloud mask output
    "fmaskcloud_ad3": f"{DATABASE_NAME}.{SCHEMA_NAME}.cemsre_t55hdv_20241008_ad3m5",
    "fmaskcloud_ad4": f"{DATABASE_NAME}.{SCHEMA_NAME}.cemsre_t55hdv_20241008_ad4m5",
    "topomasks_ad0": f"{DATABASE_NAME}.{SCHEMA_NAME}.cemsre_t55hdv_20241008_ad0m5",  # Topographic mask output
    "topomasks_ad1": f"{DATABASE_NAME}.{SCHEMA_NAME}.cemsre_t55hdv_20241008_ad1m5",

    "meta_aa0": f"{DATABASE_NAME}.{SCHEMA_NAME}.cemsre_t55hdv_20241008_aa0m5",
    "tmp_aa1": f"{DATABASE_NAME}.{SCHEMA_NAME}.cemsre_t55hdv_20241008_aa1m5",
    "tmp_aa2": f"{DATABASE_NAME}.{SCHEMA_NAME}.cemsre_t55hdv_20241008_aa2m5",
    "tmp_aa3": f"{DATABASE_NAME}.{SCHEMA_NAME}.cemsre_t55hdv_20241008_aa3m5",

    "tmp_aa2_zdirectirr": f"{DATABASE_NAME}.{SCHEMA_NAME}.cemsre_t55hdv_20241008_aa2m5_zdirectirr",
    "tmp_aa2_zdiffuseirr": f"{DATABASE_NAME}.{SCHEMA_NAME}.cemsre_t55hdv_20241008_aa2m5_zdiffuseirr",
    "tmp_aa2_zsfcrad": f"{DATABASE_NAME}.{SCHEMA_NAME}.cemsre_t55hdv_20241008_aa2m5_zsfcrad",
    "tmp_aa2_zdirectirradj": f"{DATABASE_NAME}.{SCHEMA_NAME}.cemsre_t55hdv_20241008_aa2m5_zdirectirradj",
    "tmp_aa2_zdiffuseirradj": f"{DATABASE_NAME}.{SCHEMA_NAME}.cemsre_t55hdv_20241008_aa2m5_zdiffuseirradj",
    "tmp_aa2_zincidence": f"{DATABASE_NAME}.{SCHEMA_NAME}.cemsre_t55hdv_20241008_aa2m5_zincidence",

    "tmp_aa3_zdirectirr": f"{DATABASE_NAME}.{SCHEMA_NAME}.cemsre_t55hdv_20241008_aa3m5_zdirectirr",
    "tmp_aa3_zdiffuseirr": f"{DATABASE_NAME}.{SCHEMA_NAME}.cemsre_t55hdv_20241008_aa3m5_zdiffuseirr",
    "tmp_aa3_zsfcrad": f"{DATABASE_NAME}.{SCHEMA_NAME}.cemsre_t55hdv_20241008_aa3m5_zsfcrad",
    "tmp_aa3_zdirectirradj": f"{DATABASE_NAME}.{SCHEMA_NAME}.cemsre_t55hdv_20241008_aa3m5_zdirectirradj",
    "tmp_aa3_zdiffuseirradj": f"{DATABASE_NAME}.{SCHEMA_NAME}.cemsre_t55hdv_20241008_aa3m5_zdiffuseirradj",
    "tmp_aa3_zincidence": f"{DATABASE_NAME}.{SCHEMA_NAME}.cemsre_t55hdv_20241008_aa3m5_zincidence",

    "sfcref_aba": f"{DATABASE_NAME}.{SCHEMA_NAME}.cemsre_t55hdv_20241008_abam5",  # Surface reflectance output
    "sfcref_abb": f"{DATABASE_NAME}.{SCHEMA_NAME}.cemsre_t55hdv_20241008_abbm5",
    "watermask_ad5": f"{DATABASE_NAME}.{SCHEMA_NAME}.cemsre_t55hdv_20241008_ad5m5",  # Water mask output
    "watermask_ad6": f"{DATABASE_NAME}.{SCHEMA_NAME}.cemsre_t55hdv_20241008_ad6m5",
    "fractionalcover_ac0": f"{DATABASE_NAME}.{SCHEMA_NAME}.cemsre_t55hdv_20241008_ac0m5",  # Fractional cover output
}

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
for table_fqn in TABLES.values():
    ensure_table_exists(table_fqn)

# ensure_table_exists(TABLE_X_FQN)
# ensure_table_exists(TABLE_B_FQN)
# ensure_table_exists(TABLE_Y_FQN)


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
    "test-sentinel-image-omd-lineage",
    default_args=default_args,
    description="Testing a Sentinel-2 satellite image processing pipeline with lineage",
    start_date=days_ago(1),
    is_paused_upon_creation=False,
    catchup=False,
) as dag:

    # Stage: fmaskcloud
    sentinelangle_in = DummyOperator(
        task_id="sentinelangle_input",
        inlets=[
            OMEntity(entity=Table, fqn=TABLES["meta_aa0"], key="sentinelangle"),
            #OMEntity(entity=Table, fqn=TABLES["ab1"], key="fmaskcloud"),
            #OMEntity(entity=Table, fqn=TABLES["ab2"], key="fmaskcloud"),
        ],
    )

    sentinelangle_out = DummyOperator(
        task_id="sentinelangle_output",
        outlets=[
            OMEntity(entity=Table, fqn=TABLES["tmp_aa1"], key="sentinelangle"),
            #OMEntity(entity=Table, fqn=TABLES["ab1"], key="fmaskcloud"),
            #OMEntity(entity=Table, fqn=TABLES["ab2"], key="fmaskcloud"),
        ],
    )

    toaradiance_in = DummyOperator(
        task_id="toaradiance_input",
        inlets=[
            OMEntity(entity=Table, fqn=TABLES["ab0"], key="toaradiance"),
            OMEntity(entity=Table, fqn=TABLES["tmp_aa1"], key="toaradiance"),
            #OMEntity(entity=Table, fqn=TABLES["ab2"], key="fmaskcloud"),
        ],
    )

    toaradiance_out = DummyOperator(
        task_id="toaradiance_output",
        outlets=[
            OMEntity(entity=Table, fqn=TABLES["tmp_aa2"], key="toaradiance"),
            #OMEntity(entity=Table, fqn=TABLES["ab1"], key="fmaskcloud"),
            #OMEntity(entity=Table, fqn=TABLES["ab2"], key="fmaskcloud"),
        ],
    )

    atmoscorrect_in = DummyOperator(
        task_id="atmoscorrect_input",
        inlets=[
            OMEntity(entity=Table, fqn=TABLES["tmp_aa2"], key="atmoscorrect"),
            OMEntity(entity=Table, fqn=TABLES["tmp_aa1"], key="atmoscorrect"),
            #OMEntity(entity=Table, fqn=TABLES["ab2"], key="fmaskcloud"),
        ],
    )

    atmoscorrect_out = DummyOperator(
        task_id="atmoscorrect_output",
        outlets=[
            OMEntity(entity=Table, fqn=TABLES["tmp_aa2_zdirectirr"], key="atmoscorrect"),
            OMEntity(entity=Table, fqn=TABLES["tmp_aa2_zdiffuseirr"], key="atmoscorrect"),
            #OMEntity(entity=Table, fqn=TABLES["ab2"], key="fmaskcloud"),
        ],
    )

    atmoscorrects_in = DummyOperator(
        task_id="atmoscorrects_input",
        inlets=[
            OMEntity(entity=Table, fqn=TABLES["tmp_aa2"], key="atmoscorrects"),
            OMEntity(entity=Table, fqn=TABLES["tmp_aa2_zdirectirr"], key="atmoscorrects"),
            OMEntity(entity=Table, fqn=TABLES["tmp_aa2_zdiffuseirr"], key="atmoscorrects"),
            OMEntity(entity=Table, fqn=TABLES["tmp_aa1"], key="atmoscorrects"),
        ],
    )

    atmoscorrects_out = DummyOperator(
        task_id="atmoscorrects_output",
        outlets=[
            OMEntity(entity=Table, fqn=TABLES["tmp_aa2_zsfcrad"], key="atmoscorrects"),
        ],
    )

    zincidence_in = DummyOperator(
        task_id="zincidence_input",
        inlets=[
            OMEntity(entity=Table, fqn=TABLES["tmp_aa1"], key="zincidence"),
            OMEntity(entity=Table, fqn=TABLES["ab0"], key="zincidence"),
        ],
    )

    zincidence_out = DummyOperator(
        task_id="zincidence_output",
        inlets=[
            OMEntity(entity=Table, fqn=TABLES["tmp_aa2_zincidence"], key="zincidence"),
        ],
    )

    computedirectirr_in = DummyOperator(
        task_id="computedirectirr_input",
        inlets=[
            OMEntity(entity=Table, fqn=TABLES["tmp_aa2_zdirectirr"], key="computedirectirr"),
            OMEntity(entity=Table, fqn=TABLES["tmp_aa2_zincidence"], key="computedirectirr"),
            OMEntity(entity=Table, fqn=TABLES["tmp_aa1"], key="computedirectirr"),
        ],
    )

    computedirectirr_out = DummyOperator(
        task_id="computedirectirr_output",
        outlets=[
            OMEntity(entity=Table, fqn=TABLES["tmp_aa2_zdirectirradj"], key="computedirectirr"),
        ],
    )

    adjustdiffuseirr_in = DummyOperator(
        task_id="adjustdiffuseirr_input",
        inlets=[
            OMEntity(entity=Table, fqn=TABLES["tmp_aa2_zdirectirr"], key="adjustdiffuseirr"),
            OMEntity(entity=Table, fqn=TABLES["tmp_aa2_zdiffuseirr"], key="adjustdiffuseirr"),
            OMEntity(entity=Table, fqn=TABLES["tmp_aa2_zsfcrad"], key="adjustdiffuseirr"),
        ],
    )

    adjustdiffuseirr_out = DummyOperator(
        task_id="adjustdiffuseirr_output",
        outlets=[
            OMEntity(entity=Table, fqn=TABLES["tmp_aa2_zdiffuseirradj"], key="adjustdiffuseirr"),
        ],
    )

    standardizereflectance_in = DummyOperator(
        task_id="standardizereflectance_input",
        inlets=[
            OMEntity(entity=Table, fqn=TABLES["tmp_aa2_zdirectirradj"], key="standardizereflectance"),
            OMEntity(entity=Table, fqn=TABLES["tmp_aa2_zdiffuseirradj"], key="standardizereflectance"),
            OMEntity(entity=Table, fqn=TABLES["tmp_aa2_zsfcrad"], key="standardizereflectance"),
            OMEntity(entity=Table, fqn=TABLES["tmp_aa2_zincidence"], key="standardizereflectance"),
        ],
    )

    standardizereflectance_out = DummyOperator(
        task_id="standardizereflectance_output",
        outlets=[
            OMEntity(entity=Table, fqn=TABLES["sfcref_aba"], key="standardizereflectance"),
        ],
    )

    # Define the processing sequence
    sentinelangle_in >> sentinelangle_out
    sentinelangle_out >> [toaradiance_in, zincidence_in]
    zincidence_in >> zincidence_out
    toaradiance_in >> toaradiance_out
    toaradiance_out >> atmoscorrect_in 
    atmoscorrect_in >> atmoscorrect_out
    atmoscorrect_out >> atmoscorrects_in
    atmoscorrects_in >> atmoscorrects_out
    [sentinelangle_out, atmoscorrects_out, zincidence_out] >> computedirectirr_in
    computedirectirr_in >> computedirectirr_out
    [atmoscorrect_out, atmoscorrects_out] >> adjustdiffuseirr_in
    adjustdiffuseirr_in >> adjustdiffuseirr_out
    [computedirectirr_out, atmoscorrects_out, zincidence_out, adjustdiffuseirr_out] >> standardizereflectance_in
    standardizereflectance_in >> standardizereflectance_out