from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from metadata.generated.schema.entity.services.databaseService import DatabaseService
from metadata.generated.schema.entity.data.database import Database
from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema
from metadata.generated.schema.entity.data.table import Table, Column
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

from airflow_provider_openmetadata.hooks.openmetadata import OpenMetadataHook

import json
import re

# OpenMetadata connection configuration
openmetadata_hook = OpenMetadataHook(openmetadata_conn_id="omd_connection")
server_config = openmetadata_hook.get_conn()

metadata = OpenMetadata(server_config)

# Define Service, Database, Schema
SERVICE_NAME = "airflow_dummy_service"
DATABASE_NAME = f"{SERVICE_NAME}.stac-fastapi"
SCHEMA_NAME = "SDC_Sentinel_Collection_Test2"

# Helper functions to ensure OpenMetadata entities exist
def ensure_service_exists():
    service = metadata.get_by_name(entity=DatabaseService, fqn=SERVICE_NAME)
    if not service:
        print(f"Service '{SERVICE_NAME}' not found. Creating it...")
        service_request = CreateDatabaseServiceRequest(
            name=SERVICE_NAME,
            serviceType="Postgres"
        )
        metadata.create_or_update(service_request)
        print(f"Service '{SERVICE_NAME}' created.")

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

def ensure_table_exists(table_name, table_fqn):
    schema = metadata.get_by_name(entity=DatabaseSchema, fqn=f"{DATABASE_NAME}.{SCHEMA_NAME}")
    table = metadata.get_by_name(entity=Table, fqn=table_fqn)
    if not table:
        print(f"Table '{table_fqn}' not found. Creating it...")
        table_request = CreateTableRequest(
            name=table_name,
            databaseSchema=schema.fullyQualifiedName,
            columns=[Column(name="type", dataType="STRING", description="Satellite image data")]
        )
        metadata.create_or_update(table_request)
        print(f"Table '{table_fqn}' created.")

# Parse JSON to extract TABLES and processing steps
def parse_json_data(data, tables=None, steps=None):
    if tables is None:
        tables = {}
    if steps is None:
        steps = {}

    for output_file, details in data.items():
        # Extract table name from filename (strip path and timestamp)
        table_name = output_file.split('/')[-1]
        table_fqn = f"{DATABASE_NAME}.{SCHEMA_NAME}.{table_name}"
        tables[output_file] = table_fqn

        script = details["metadata"]["script"]
        if not script:  # Skip raw inputs with empty script
            continue

        # Sanitize script name for task_id
        script_id = script.replace(".py", "").replace("_", "-").lower()
        if script_id not in steps:
            steps[script_id] = {"inputs": set(), "outputs": set(), "raw_inputs": []}

        # Add output
        steps[script_id]["outputs"].add(table_fqn)

        # Process inputs
        for input_dict in details["input"]:
            input_file = list(input_dict.keys())[0]
            input_fqn = f"{DATABASE_NAME}.{SCHEMA_NAME}.{input_file.split('/')[-1]}"
            tables[input_file] = input_fqn
            # If this input has no script, it's a raw input
            if not input_dict[input_file]["metadata"]["script"]:
                steps[script_id]["raw_inputs"].append(input_fqn)
            else:
                steps[script_id]["inputs"].add(input_fqn)
            # Recursively parse inputs
            parse_json_data(input_dict, tables, steps)

    return tables, steps

# Load JSON data (replace with actual file path or inline JSON)
json_data = {
    "cgmsre_t54hvh_20250211_ad6m4.img": {
        "filename": "cgmsre_t54hvh_20250211_ad6m4.img 2025-02-13 00:28:34",
        "metadata": {
            "script": "qv_water_index2015.py"
        },
        "input": [
            {
                "cgmsre_t54hvh_20250211_abam4.img": {
                    "filename": "cgmsre_t54hvh_20250211_abam4.img 2025-02-13 00:26:20",
                    "metadata": {
                        "script": "standardizereflectance.py"
                    },
                    "input": [
                        {
                            "cgmsre_t54hvh_20250211_aa2m4_zsfcrad.img": {
                                "filename": "cgmsre_t54hvh_20250211_aa2m4_zsfcrad.img 2025-02-13 00:24:56",
                                "metadata": {
                                    "script": "atmoscorrectsentinel2.py"
                                },
                                "input": [
                                    {
                                        "cgmsre_t54hvh_20250211_aa2m4.img": {
                                            "filename": "cgmsre_t54hvh_20250211_aa2m4.img 2025-02-13 00:21:50",
                                            "metadata": {
                                                "script": "toaradiance_sen2.py"
                                            },
                                            "input": [
                                                {
                                                    "cgmsre_t54hvh_20250211_ab0m4.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_ab0m4.img 2025-02-13 00:11:48",
                                                        "metadata": {
                                                            "script": "qv_importsentinel2_l1c.py"
                                                        },
                                                        "input": []
                                                    }
                                                },
                                                {
                                                    "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:21:32",
                                                        "metadata": {
                                                            "script": "qv_sentinel2angles.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:21:32",
                                                                    "metadata": {
                                                                        "script": ""
                                                                    },
                                                                    "input": []
                                                                }
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        }
                                    },
                                    {
                                        "/mnt/scratch_lustre/tmp/rs_testing/download/tmpjmpxe79k.img": {
                                            "filename": "/mnt/scratch_lustre/tmp/rs_testing/download/tmpjmpxe79k.img 2025-02-13 00:23:39",
                                            "metadata": {
                                                "script": "atmoscorrectsentinel2.py"
                                            },
                                            "input": [
                                                {
                                                    "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif": {
                                                        "filename": "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif 2014-05-01 16:58:59",
                                                        "metadata": {
                                                            "script": "extendAusDEM.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "shraem_aus_20000211_ac0g0.tif": {
                                                                    "filename": "shraem_aus_20000211_ac0g0.tif 2010-03-25 22:27:54",
                                                                    "metadata": {
                                                                        "script": "insertDEMHistory.py"
                                                                    },
                                                                    "input": []
                                                                }
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        }
                                    },
                                    {
                                        "cgmsre_t54hvh_20250211_aa2m4_zdirectirr.img": {
                                            "filename": "cgmsre_t54hvh_20250211_aa2m4_zdirectirr.img 2025-02-13 00:24:03",
                                            "metadata": {
                                                "script": "atmoscorrectsentinel2.py"
                                            },
                                            "input": [
                                                {
                                                    "cgmsre_t54hvh_20250211_aa2m4.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa2m4.img 2025-02-13 00:21:50",
                                                        "metadata": {
                                                            "script": "toaradiance_sen2.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "cgmsre_t54hvh_20250211_ab0m4.img": {
                                                                    "filename": "cgmsre_t54hvh_20250211_ab0m4.img 2025-02-13 00:11:48",
                                                                    "metadata": {
                                                                        "script": "qv_importsentinel2_l1c.py"
                                                                    },
                                                                    "input": []
                                                                }
                                                            },
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:21:32",
                                                                    "metadata": {
                                                                        "script": "qv_sentinel2angles.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                                "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:21:32",
                                                                                "metadata": {
                                                                                    "script": ""
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            }
                                                        ]
                                                    }
                                                },
                                                {
                                                    "/mnt/scratch_lustre/tmp/rs_testing/download/tmpjmpxe79k.img": {
                                                        "filename": "/mnt/scratch_lustre/tmp/rs_testing/download/tmpjmpxe79k.img 2025-02-13 00:23:39",
                                                        "metadata": {
                                                            "script": "atmoscorrectsentinel2.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif": {
                                                                    "filename": "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif 2014-05-01 16:58:59",
                                                                    "metadata": {
                                                                        "script": "extendAusDEM.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "shraem_aus_20000211_ac0g0.tif": {
                                                                                "filename": "shraem_aus_20000211_ac0g0.tif 2010-03-25 22:27:54",
                                                                                "metadata": {
                                                                                    "script": "insertDEMHistory.py"
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            }
                                                        ]
                                                    }
                                                },
                                                {
                                                    "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:21:32",
                                                        "metadata": {
                                                            "script": "qv_sentinel2angles.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:21:32",
                                                                    "metadata": {
                                                                        "script": ""
                                                                    },
                                                                    "input": []
                                                                }
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        }
                                    },
                                    {
                                        "cgmsre_t54hvh_20250211_aa2m4_zdiffuseirr.img": {
                                            "filename": "cgmsre_t54hvh_20250211_aa2m4_zdiffuseirr.img 2025-02-13 00:24:22",
                                            "metadata": {
                                                "script": "atmoscorrectsentinel2.py"
                                            },
                                            "input": [
                                                {
                                                    "cgmsre_t54hvh_20250211_aa2m4.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa2m4.img 2025-02-13 00:21:50",
                                                        "metadata": {
                                                            "script": "toaradiance_sen2.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "cgmsre_t54hvh_20250211_ab0m4.img": {
                                                                    "filename": "cgmsre_t54hvh_20250211_ab0m4.img 2025-02-13 00:11:48",
                                                                    "metadata": {
                                                                        "script": "qv_importsentinel2_l1c.py"
                                                                    },
                                                                    "input": []
                                                                }
                                                            },
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:21:32",
                                                                    "metadata": {
                                                                        "script": "qv_sentinel2angles.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                                "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:21:32",
                                                                                "metadata": {
                                                                                    "script": ""
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            }
                                                        ]
                                                    }
                                                },
                                                {
                                                    "/mnt/scratch_lustre/tmp/rs_testing/download/tmpjmpxe79k.img": {
                                                        "filename": "/mnt/scratch_lustre/tmp/rs_testing/download/tmpjmpxe79k.img 2025-02-13 00:23:39",
                                                        "metadata": {
                                                            "script": "atmoscorrectsentinel2.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif": {
                                                                    "filename": "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif 2014-05-01 16:58:59",
                                                                    "metadata": {
                                                                        "script": "extendAusDEM.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "shraem_aus_20000211_ac0g0.tif": {
                                                                                "filename": "shraem_aus_20000211_ac0g0.tif 2010-03-25 22:27:54",
                                                                                "metadata": {
                                                                                    "script": "insertDEMHistory.py"
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            }
                                                        ]
                                                    }
                                                },
                                                {
                                                    "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:21:32",
                                                        "metadata": {
                                                            "script": "qv_sentinel2angles.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:21:32",
                                                                    "metadata": {
                                                                        "script": ""
                                                                    },
                                                                    "input": []
                                                                }
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        }
                                    },
                                    {
                                        "cgmsre_t54hvh_20250211_aa1m4.img": {
                                            "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:21:32",
                                            "metadata": {
                                                "script": "qv_sentinel2angles.py"
                                            },
                                            "input": [
                                                {
                                                    "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:21:32",
                                                        "metadata": {
                                                            "script": ""
                                                        },
                                                        "input": []
                                                    }
                                                }
                                            ]
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "cgmsre_t54hvh_20250211_aa2m4_zdirectirradj.img": {
                                "filename": "cgmsre_t54hvh_20250211_aa2m4_zdirectirradj.img 2025-02-13 00:25:18",
                                "metadata": {
                                    "script": "computedirectirr.py"
                                },
                                "input": [
                                    {
                                        "cgmsre_t54hvh_20250211_aa2m4_zdirectirr.img": {
                                            "filename": "cgmsre_t54hvh_20250211_aa2m4_zdirectirr.img 2025-02-13 00:24:03",
                                            "metadata": {
                                                "script": "atmoscorrectsentinel2.py"
                                            },
                                            "input": [
                                                {
                                                    "cgmsre_t54hvh_20250211_aa2m4.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa2m4.img 2025-02-13 00:21:50",
                                                        "metadata": {
                                                            "script": "toaradiance_sen2.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "cgmsre_t54hvh_20250211_ab0m4.img": {
                                                                    "filename": "cgmsre_t54hvh_20250211_ab0m4.img 2025-02-13 00:11:48",
                                                                    "metadata": {
                                                                        "script": "qv_importsentinel2_l1c.py"
                                                                    },
                                                                    "input": []
                                                                }
                                                            },
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:21:32",
                                                                    "metadata": {
                                                                        "script": "qv_sentinel2angles.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                                "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:21:32",
                                                                                "metadata": {
                                                                                    "script": ""
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            }
                                                        ]
                                                    }
                                                },
                                                {
                                                    "/mnt/scratch_lustre/tmp/rs_testing/download/tmpjmpxe79k.img": {
                                                        "filename": "/mnt/scratch_lustre/tmp/rs_testing/download/tmpjmpxe79k.img 2025-02-13 00:23:39",
                                                        "metadata": {
                                                            "script": "atmoscorrectsentinel2.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif": {
                                                                    "filename": "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif 2014-05-01 16:58:59",
                                                                    "metadata": {
                                                                        "script": "extendAusDEM.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "shraem_aus_20000211_ac0g0.tif": {
                                                                                "filename": "shraem_aus_20000211_ac0g0.tif 2010-03-25 22:27:54",
                                                                                "metadata": {
                                                                                    "script": "insertDEMHistory.py"
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            }
                                                        ]
                                                    }
                                                },
                                                {
                                                    "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:21:32",
                                                        "metadata": {
                                                            "script": "qv_sentinel2angles.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:21:32",
                                                                    "metadata": {
                                                                        "script": ""
                                                                    },
                                                                    "input": []
                                                                }
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        }
                                    },
                                    {
                                        "cgmsre_t54hvh_20250211_aa2m4_zincidence.img": {
                                            "filename": "cgmsre_t54hvh_20250211_aa2m4_zincidence.img 2025-02-13 00:23:32",
                                            "metadata": {
                                                "script": "makeincidenceangles.py"
                                            },
                                            "input": [
                                                {
                                                    "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:21:32",
                                                        "metadata": {
                                                            "script": "qv_sentinel2angles.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:21:32",
                                                                    "metadata": {
                                                                        "script": ""
                                                                    },
                                                                    "input": []
                                                                }
                                                            }
                                                        ]
                                                    }
                                                },
                                                {
                                                    "cgmsre_t54hvh_20250211_ab0m4_zdem.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_ab0m4_zdem.img 2025-02-13 00:22:06",
                                                        "metadata": {
                                                            "script": "demlike.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif": {
                                                                    "filename": "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif 2014-05-01 16:58:59",
                                                                    "metadata": {
                                                                        "script": "extendAusDEM.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "shraem_aus_20000211_ac0g0.tif": {
                                                                                "filename": "shraem_aus_20000211_ac0g0.tif 2010-03-25 22:27:54",
                                                                                "metadata": {
                                                                                    "script": "insertDEMHistory.py"
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        }
                                    },
                                    {
                                        "cgmsre_t54hvh_20250211_aa1m4.img": {
                                            "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:21:32",
                                            "metadata": {
                                                "script": "qv_sentinel2angles.py"
                                            },
                                            "input": [
                                                {
                                                    "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:21:32",
                                                        "metadata": {
                                                            "script": ""
                                                        },
                                                        "input": []
                                                    }
                                                }
                                            ]
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "cgmsre_t54hvh_20250211_aa2m4_zdiffuseirradj.img": {
                                "filename": "cgmsre_t54hvh_20250211_aa2m4_zdiffuseirradj.img 2025-02-13 00:25:36",
                                "metadata": {
                                    "script": "adjustdiffuseirr.py"
                                },
                                "input": [
                                    {
                                        "cgmsre_t54hvh_20250211_aa2m4_zdirectirr.img": {
                                            "filename": "cgmsre_t54hvh_20250211_aa2m4_zdirectirr.img 2025-02-13 00:24:03",
                                            "metadata": {
                                                "script": "atmoscorrectsentinel2.py"
                                            },
                                            "input": [
                                                {
                                                    "cgmsre_t54hvh_20250211_aa2m4.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa2m4.img 2025-02-13 00:21:50",
                                                        "metadata": {
                                                            "script": "toaradiance_sen2.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "cgmsre_t54hvh_20250211_ab0m4.img": {
                                                                    "filename": "cgmsre_t54hvh_20250211_ab0m4.img 2025-02-13 00:11:48",
                                                                    "metadata": {
                                                                        "script": "qv_importsentinel2_l1c.py"
                                                                    },
                                                                    "input": []
                                                                }
                                                            },
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:21:32",
                                                                    "metadata": {
                                                                        "script": "qv_sentinel2angles.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                                "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:21:32",
                                                                                "metadata": {
                                                                                    "script": ""
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            }
                                                        ]
                                                    }
                                                },
                                                {
                                                    "/mnt/scratch_lustre/tmp/rs_testing/download/tmpjmpxe79k.img": {
                                                        "filename": "/mnt/scratch_lustre/tmp/rs_testing/download/tmpjmpxe79k.img 2025-02-13 00:23:39",
                                                        "metadata": {
                                                            "script": "atmoscorrectsentinel2.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif": {
                                                                    "filename": "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif 2014-05-01 16:58:59",
                                                                    "metadata": {
                                                                        "script": "extendAusDEM.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "shraem_aus_20000211_ac0g0.tif": {
                                                                                "filename": "shraem_aus_20000211_ac0g0.tif 2010-03-25 22:27:54",
                                                                                "metadata": {
                                                                                    "script": "insertDEMHistory.py"
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            }
                                                        ]
                                                    }
                                                },
                                                {
                                                    "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:21:32",
                                                        "metadata": {
                                                            "script": "qv_sentinel2angles.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:21:32",
                                                                    "metadata": {
                                                                        "script": ""
                                                                    },
                                                                    "input": []
                                                                }
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        }
                                    },
                                    {
                                        "cgmsre_t54hvh_20250211_aa2m4_zdiffuseirr.img": {
                                            "filename": "cgmsre_t54hvh_20250211_aa2m4_zdiffuseirr.img 2025-02-13 00:24:22",
                                            "metadata": {
                                                "script": "atmoscorrectsentinel2.py"
                                            },
                                            "input": [
                                                {
                                                    "cgmsre_t54hvh_20250211_aa2m4.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa2m4.img 2025-02-13 00:21:50",
                                                        "metadata": {
                                                            "script": "toaradiance_sen2.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "cgmsre_t54hvh_20250211_ab0m4.img": {
                                                                    "filename": "cgmsre_t54hvh_20250211_ab0m4.img 2025-02-13 00:11:48",
                                                                    "metadata": {
                                                                        "script": "qv_importsentinel2_l1c.py"
                                                                    },
                                                                    "input": []
                                                                }
                                                            },
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:21:32",
                                                                    "metadata": {
                                                                        "script": "qv_sentinel2angles.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                                "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:21:32",
                                                                                "metadata": {
                                                                                    "script": ""
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            }
                                                        ]
                                                    }
                                                },
                                                {
                                                    "/mnt/scratch_lustre/tmp/rs_testing/download/tmpjmpxe79k.img": {
                                                        "filename": "/mnt/scratch_lustre/tmp/rs_testing/download/tmpjmpxe79k.img 2025-02-13 00:23:39",
                                                        "metadata": {
                                                            "script": "atmoscorrectsentinel2.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif": {
                                                                    "filename": "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif 2014-05-01 16:58:59",
                                                                    "metadata": {
                                                                        "script": "extendAusDEM.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "shraem_aus_20000211_ac0g0.tif": {
                                                                                "filename": "shraem_aus_20000211_ac0g0.tif 2010-03-25 22:27:54",
                                                                                "metadata": {
                                                                                    "script": "insertDEMHistory.py"
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            }
                                                        ]
                                                    }
                                                },
                                                {
                                                    "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:21:32",
                                                        "metadata": {
                                                            "script": "qv_sentinel2angles.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:21:32",
                                                                    "metadata": {
                                                                        "script": ""
                                                                    },
                                                                    "input": []
                                                                }
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        }
                                    },
                                    {
                                        "cgmsre_t54hvh_20250211_aa2m4_zsfcrad.img": {
                                            "filename": "cgmsre_t54hvh_20250211_aa2m4_zsfcrad.img 2025-02-13 00:24:56",
                                            "metadata": {
                                                "script": "atmoscorrectsentinel2.py"
                                            },
                                            "input": [
                                                {
                                                    "cgmsre_t54hvh_20250211_aa2m4.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa2m4.img 2025-02-13 00:21:50",
                                                        "metadata": {
                                                            "script": "toaradiance_sen2.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "cgmsre_t54hvh_20250211_ab0m4.img": {
                                                                    "filename": "cgmsre_t54hvh_20250211_ab0m4.img 2025-02-13 00:11:48",
                                                                    "metadata": {
                                                                        "script": "qv_importsentinel2_l1c.py"
                                                                    },
                                                                    "input": []
                                                                }
                                                            },
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:21:32",
                                                                    "metadata": {
                                                                        "script": "qv_sentinel2angles.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                                "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:21:32",
                                                                                "metadata": {
                                                                                    "script": ""
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            }
                                                        ]
                                                    }
                                                },
                                                {
                                                    "/mnt/scratch_lustre/tmp/rs_testing/download/tmpjmpxe79k.img": {
                                                        "filename": "/mnt/scratch_lustre/tmp/rs_testing/download/tmpjmpxe79k.img 2025-02-13 00:23:39",
                                                        "metadata": {
                                                            "script": "atmoscorrectsentinel2.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif": {
                                                                    "filename": "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif 2014-05-01 16:58:59",
                                                                    "metadata": {
                                                                        "script": "extendAusDEM.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "shraem_aus_20000211_ac0g0.tif": {
                                                                                "filename": "shraem_aus_20000211_ac0g0.tif 2010-03-25 22:27:54",
                                                                                "metadata": {
                                                                                    "script": "insertDEMHistory.py"
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            }
                                                        ]
                                                    }
                                                },
                                                {
                                                    "cgmsre_t54hvh_20250211_aa2m4_zdirectirr.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa2m4_zdirectirr.img 2025-02-13 00:24:03",
                                                        "metadata": {
                                                            "script": "atmoscorrectsentinel2.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa2m4.img": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa2m4.img 2025-02-13 00:21:50",
                                                                    "metadata": {
                                                                        "script": "toaradiance_sen2.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "cgmsre_t54hvh_20250211_ab0m4.img": {
                                                                                "filename": "cgmsre_t54hvh_20250211_ab0m4.img 2025-02-13 00:11:48",
                                                                                "metadata": {
                                                                                    "script": "qv_importsentinel2_l1c.py"
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        },
                                                                        {
                                                                            "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                                                "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:21:32",
                                                                                "metadata": {
                                                                                    "script": "qv_sentinel2angles.py"
                                                                                },
                                                                                "input": [
                                                                                    {
                                                                                        "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                                            "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:21:32",
                                                                                            "metadata": {
                                                                                                "script": ""
                                                                                            },
                                                                                            "input": []
                                                                                        }
                                                                                    }
                                                                                ]
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            },
                                                            {
                                                                "/mnt/scratch_lustre/tmp/rs_testing/download/tmpjmpxe79k.img": {
                                                                    "filename": "/mnt/scratch_lustre/tmp/rs_testing/download/tmpjmpxe79k.img 2025-02-13 00:23:39",
                                                                    "metadata": {
                                                                        "script": "atmoscorrectsentinel2.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif": {
                                                                                "filename": "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif 2014-05-01 16:58:59",
                                                                                "metadata": {
                                                                                    "script": "extendAusDEM.py"
                                                                                },
                                                                                "input": [
                                                                                    {
                                                                                        "shraem_aus_20000211_ac0g0.tif": {
                                                                                            "filename": "shraem_aus_20000211_ac0g0.tif 2010-03-25 22:27:54",
                                                                                            "metadata": {
                                                                                                "script": "insertDEMHistory.py"
                                                                                            },
                                                                                            "input": []
                                                                                        }
                                                                                    }
                                                                                ]
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            },
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:21:32",
                                                                    "metadata": {
                                                                        "script": "qv_sentinel2angles.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                                "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:21:32",
                                                                                "metadata": {
                                                                                    "script": ""
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            }
                                                        ]
                                                    }
                                                },
                                                {
                                                    "cgmsre_t54hvh_20250211_aa2m4_zdiffuseirr.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa2m4_zdiffuseirr.img 2025-02-13 00:24:22",
                                                        "metadata": {
                                                            "script": "atmoscorrectsentinel2.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa2m4.img": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa2m4.img 2025-02-13 00:21:50",
                                                                    "metadata": {
                                                                        "script": "toaradiance_sen2.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "cgmsre_t54hvh_20250211_ab0m4.img": {
                                                                                "filename": "cgmsre_t54hvh_20250211_ab0m4.img 2025-02-13 00:11:48",
                                                                                "metadata": {
                                                                                    "script": "qv_importsentinel2_l1c.py"
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        },
                                                                        {
                                                                            "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                                                "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:21:32",
                                                                                "metadata": {
                                                                                    "script": "qv_sentinel2angles.py"
                                                                                },
                                                                                "input": [
                                                                                    {
                                                                                        "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                                            "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:21:32",
                                                                                            "metadata": {
                                                                                                "script": ""
                                                                                            },
                                                                                            "input": []
                                                                                        }
                                                                                    }
                                                                                ]
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            },
                                                            {
                                                                "/mnt/scratch_lustre/tmp/rs_testing/download/tmpjmpxe79k.img": {
                                                                    "filename": "/mnt/scratch_lustre/tmp/rs_testing/download/tmpjmpxe79k.img 2025-02-13 00:23:39",
                                                                    "metadata": {
                                                                        "script": "atmoscorrectsentinel2.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif": {
                                                                                "filename": "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif 2014-05-01 16:58:59",
                                                                                "metadata": {
                                                                                    "script": "extendAusDEM.py"
                                                                                },
                                                                                "input": [
                                                                                    {
                                                                                        "shraem_aus_20000211_ac0g0.tif": {
                                                                                            "filename": "shraem_aus_20000211_ac0g0.tif 2010-03-25 22:27:54",
                                                                                            "metadata": {
                                                                                                "script": "insertDEMHistory.py"
                                                                                            },
                                                                                            "input": []
                                                                                        }
                                                                                    }
                                                                                ]
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            },
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:21:32",
                                                                    "metadata": {
                                                                        "script": "qv_sentinel2angles.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                                "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:21:32",
                                                                                "metadata": {
                                                                                    "script": ""
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            }
                                                        ]
                                                    }
                                                },
                                                {
                                                    "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:21:32",
                                                        "metadata": {
                                                            "script": "qv_sentinel2angles.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:21:32",
                                                                    "metadata": {
                                                                        "script": ""
                                                                    },
                                                                    "input": []
                                                                }
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        }
                                    },
                                    {
                                        "cgmsre_t54hvh_20250211_da5m4_zdiffuseirr.img": {
                                            "filename": "cgmsre_t54hvh_20250211_da5m4_zdiffuseirr.img 2025-02-13 00:25:19",
                                            "metadata": {
                                                "script": "adjustdiffuseirr.py"
                                            },
                                            "input": [
                                                {
                                                    "/mnt/project/refData/skyview/lztmre_auszone54_eall_da5m4.tif": {
                                                        "filename": "/mnt/project/refData/skyview/lztmre_auszone54_eall_da5m4.tif 2012-04-03 11:00:22",
                                                        "metadata": {
                                                            "script": "wholeZoneViewFactor.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "/apollo/data/images/geog/shraem_aus_20000211_ac0g0.tif": {
                                                                    "filename": "/apollo/data/images/geog/shraem_aus_20000211_ac0g0.tif 2010-03-25 22:27:54",
                                                                    "metadata": {
                                                                        "script": "insertDEMHistory.py"
                                                                    },
                                                                    "input": []
                                                                }
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "cgmsre_t54hvh_20250211_aa2m4_zincidence.img": {
                                "filename": "cgmsre_t54hvh_20250211_aa2m4_zincidence.img 2025-02-13 00:23:32",
                                "metadata": {
                                    "script": "makeincidenceangles.py"
                                },
                                "input": [
                                    {
                                        "cgmsre_t54hvh_20250211_aa1m4.img": {
                                            "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:21:32",
                                            "metadata": {
                                                "script": "qv_sentinel2angles.py"
                                            },
                                            "input": [
                                                {
                                                    "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:21:32",
                                                        "metadata": {
                                                            "script": ""
                                                        },
                                                        "input": []
                                                    }
                                                }
                                            ]
                                        }
                                    },
                                    {
                                        "cgmsre_t54hvh_20250211_ab0m4_zdem.img": {
                                            "filename": "cgmsre_t54hvh_20250211_ab0m4_zdem.img 2025-02-13 00:22:06",
                                            "metadata": {
                                                "script": "demlike.py"
                                            },
                                            "input": [
                                                {
                                                    "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif": {
                                                        "filename": "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif 2014-05-01 16:58:59",
                                                        "metadata": {
                                                            "script": "extendAusDEM.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "shraem_aus_20000211_ac0g0.tif": {
                                                                    "filename": "shraem_aus_20000211_ac0g0.tif 2010-03-25 22:27:54",
                                                                    "metadata": {
                                                                        "script": "insertDEMHistory.py"
                                                                    },
                                                                    "input": []
                                                                }
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "/mnt/appsource/local/rs/rsc/brdf/paramfiles/Sentinel2MSI10m.hsdref": {
                                "filename": "/mnt/appsource/local/rs/rsc/brdf/paramfiles/Sentinel2MSI10m.hsdref 2016-02-06 13:10:22",
                                "metadata": {
                                    "script": "calcHemisphericDirectionalRef.py"
                                },
                                "input": [
                                    {
                                        "Sentinel2MSI10m.rtlsparams": {
                                            "filename": "Sentinel2MSI10m.rtlsparams 2016-02-06 13:07:59",
                                            "metadata": {
                                                "script": " "
                                            },
                                            "input": [
                                                {
                                                    "SPOT_HRG.rtlsparams": {
                                                        "filename": "SPOT_HRG.rtlsparams 2011-08-05 10:56:28",
                                                        "metadata": {
                                                            "script": "fit_brdf.py"
                                                        },
                                                        "input": []
                                                    }
                                                },
                                                {
                                                    "LandsatTM.rtlsparams": {
                                                        "filename": "LandsatTM.rtlsparams 2011-04-14 15:24:36",
                                                        "metadata": {
                                                            "script": "fit_brdf.py"
                                                        },
                                                        "input": []
                                                    }
                                                }
                                            ]
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                }
            },
            {
                "cgmsre_t54hvh_20250211_abbm4.img": {
                    "filename": "cgmsre_t54hvh_20250211_abbm4.img 2025-02-13 00:27:58",
                    "metadata": {
                        "script": "standardizereflectance.py"
                    },
                    "input": [
                        {
                            "cgmsre_t54hvh_20250211_aa3m4_zsfcrad.img": {
                                "filename": "cgmsre_t54hvh_20250211_aa3m4_zsfcrad.img 2025-02-13 00:27:28",
                                "metadata": {
                                    "script": "atmoscorrectsentinel2.py"
                                },
                                "input": [
                                    {
                                        "cgmsre_t54hvh_20250211_aa3m4.img": {
                                            "filename": "cgmsre_t54hvh_20250211_aa3m4.img 2025-02-13 00:26:28",
                                            "metadata": {
                                                "script": "toaradiance_sen2.py"
                                            },
                                            "input": [
                                                {
                                                    "cgmsre_t54hvh_20250211_ab1m4.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_ab1m4.img 2025-02-13 00:11:51",
                                                        "metadata": {
                                                            "script": "qv_importsentinel2_l1c.py"
                                                        },
                                                        "input": []
                                                    }
                                                },
                                                {
                                                    "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:26:21",
                                                        "metadata": {
                                                            "script": "qv_sentinel2angles.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:26:21",
                                                                    "metadata": {
                                                                        "script": ""
                                                                    },
                                                                    "input": []
                                                                }
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        }
                                    },
                                    {
                                        "/mnt/scratch_lustre/tmp/rs_testing/download/tmpmm6jt_f5.img": {
                                            "filename": "/mnt/scratch_lustre/tmp/rs_testing/download/tmpmm6jt_f5.img 2025-02-13 00:26:58",
                                            "metadata": {
                                                "script": "atmoscorrectsentinel2.py"
                                            },
                                            "input": [
                                                {
                                                    "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif": {
                                                        "filename": "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif 2014-05-01 16:58:59",
                                                        "metadata": {
                                                            "script": "extendAusDEM.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "shraem_aus_20000211_ac0g0.tif": {
                                                                    "filename": "shraem_aus_20000211_ac0g0.tif 2010-03-25 22:27:54",
                                                                    "metadata": {
                                                                        "script": "insertDEMHistory.py"
                                                                    },
                                                                    "input": []
                                                                }
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        }
                                    },
                                    {
                                        "cgmsre_t54hvh_20250211_aa3m4_zdirectirr.img": {
                                            "filename": "cgmsre_t54hvh_20250211_aa3m4_zdirectirr.img 2025-02-13 00:27:10",
                                            "metadata": {
                                                "script": "atmoscorrectsentinel2.py"
                                            },
                                            "input": [
                                                {
                                                    "cgmsre_t54hvh_20250211_aa3m4.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa3m4.img 2025-02-13 00:26:28",
                                                        "metadata": {
                                                            "script": "toaradiance_sen2.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "cgmsre_t54hvh_20250211_ab1m4.img": {
                                                                    "filename": "cgmsre_t54hvh_20250211_ab1m4.img 2025-02-13 00:11:51",
                                                                    "metadata": {
                                                                        "script": "qv_importsentinel2_l1c.py"
                                                                    },
                                                                    "input": []
                                                                }
                                                            },
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:26:21",
                                                                    "metadata": {
                                                                        "script": "qv_sentinel2angles.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                                "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:26:21",
                                                                                "metadata": {
                                                                                    "script": ""
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            }
                                                        ]
                                                    }
                                                },
                                                {
                                                    "/mnt/scratch_lustre/tmp/rs_testing/download/tmpmm6jt_f5.img": {
                                                        "filename": "/mnt/scratch_lustre/tmp/rs_testing/download/tmpmm6jt_f5.img 2025-02-13 00:26:58",
                                                        "metadata": {
                                                            "script": "atmoscorrectsentinel2.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif": {
                                                                    "filename": "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif 2014-05-01 16:58:59",
                                                                    "metadata": {
                                                                        "script": "extendAusDEM.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "shraem_aus_20000211_ac0g0.tif": {
                                                                                "filename": "shraem_aus_20000211_ac0g0.tif 2010-03-25 22:27:54",
                                                                                "metadata": {
                                                                                    "script": "insertDEMHistory.py"
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            }
                                                        ]
                                                    }
                                                },
                                                {
                                                    "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:26:21",
                                                        "metadata": {
                                                            "script": "qv_sentinel2angles.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:26:21",
                                                                    "metadata": {
                                                                        "script": ""
                                                                    },
                                                                    "input": []
                                                                }
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        }
                                    },
                                    {
                                        "cgmsre_t54hvh_20250211_aa3m4_zdiffuseirr.img": {
                                            "filename": "cgmsre_t54hvh_20250211_aa3m4_zdiffuseirr.img 2025-02-13 00:27:16",
                                            "metadata": {
                                                "script": "atmoscorrectsentinel2.py"
                                            },
                                            "input": [
                                                {
                                                    "cgmsre_t54hvh_20250211_aa3m4.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa3m4.img 2025-02-13 00:26:28",
                                                        "metadata": {
                                                            "script": "toaradiance_sen2.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "cgmsre_t54hvh_20250211_ab1m4.img": {
                                                                    "filename": "cgmsre_t54hvh_20250211_ab1m4.img 2025-02-13 00:11:51",
                                                                    "metadata": {
                                                                        "script": "qv_importsentinel2_l1c.py"
                                                                    },
                                                                    "input": []
                                                                }
                                                            },
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:26:21",
                                                                    "metadata": {
                                                                        "script": "qv_sentinel2angles.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                                "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:26:21",
                                                                                "metadata": {
                                                                                    "script": ""
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            }
                                                        ]
                                                    }
                                                },
                                                {
                                                    "/mnt/scratch_lustre/tmp/rs_testing/download/tmpmm6jt_f5.img": {
                                                        "filename": "/mnt/scratch_lustre/tmp/rs_testing/download/tmpmm6jt_f5.img 2025-02-13 00:26:58",
                                                        "metadata": {
                                                            "script": "atmoscorrectsentinel2.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif": {
                                                                    "filename": "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif 2014-05-01 16:58:59",
                                                                    "metadata": {
                                                                        "script": "extendAusDEM.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "shraem_aus_20000211_ac0g0.tif": {
                                                                                "filename": "shraem_aus_20000211_ac0g0.tif 2010-03-25 22:27:54",
                                                                                "metadata": {
                                                                                    "script": "insertDEMHistory.py"
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            }
                                                        ]
                                                    }
                                                },
                                                {
                                                    "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:26:21",
                                                        "metadata": {
                                                            "script": "qv_sentinel2angles.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:26:21",
                                                                    "metadata": {
                                                                        "script": ""
                                                                    },
                                                                    "input": []
                                                                }
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        }
                                    },
                                    {
                                        "cgmsre_t54hvh_20250211_aa1m4.img": {
                                            "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:26:21",
                                            "metadata": {
                                                "script": "qv_sentinel2angles.py"
                                            },
                                            "input": [
                                                {
                                                    "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:26:21",
                                                        "metadata": {
                                                            "script": ""
                                                        },
                                                        "input": []
                                                    }
                                                }
                                            ]
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "cgmsre_t54hvh_20250211_aa3m4_zdirectirradj.img": {
                                "filename": "cgmsre_t54hvh_20250211_aa3m4_zdirectirradj.img 2025-02-13 00:27:36",
                                "metadata": {
                                    "script": "computedirectirr.py"
                                },
                                "input": [
                                    {
                                        "cgmsre_t54hvh_20250211_aa3m4_zdirectirr.img": {
                                            "filename": "cgmsre_t54hvh_20250211_aa3m4_zdirectirr.img 2025-02-13 00:27:10",
                                            "metadata": {
                                                "script": "atmoscorrectsentinel2.py"
                                            },
                                            "input": [
                                                {
                                                    "cgmsre_t54hvh_20250211_aa3m4.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa3m4.img 2025-02-13 00:26:28",
                                                        "metadata": {
                                                            "script": "toaradiance_sen2.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "cgmsre_t54hvh_20250211_ab1m4.img": {
                                                                    "filename": "cgmsre_t54hvh_20250211_ab1m4.img 2025-02-13 00:11:51",
                                                                    "metadata": {
                                                                        "script": "qv_importsentinel2_l1c.py"
                                                                    },
                                                                    "input": []
                                                                }
                                                            },
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:26:21",
                                                                    "metadata": {
                                                                        "script": "qv_sentinel2angles.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                                "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:26:21",
                                                                                "metadata": {
                                                                                    "script": ""
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            }
                                                        ]
                                                    }
                                                },
                                                {
                                                    "/mnt/scratch_lustre/tmp/rs_testing/download/tmpmm6jt_f5.img": {
                                                        "filename": "/mnt/scratch_lustre/tmp/rs_testing/download/tmpmm6jt_f5.img 2025-02-13 00:26:58",
                                                        "metadata": {
                                                            "script": "atmoscorrectsentinel2.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif": {
                                                                    "filename": "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif 2014-05-01 16:58:59",
                                                                    "metadata": {
                                                                        "script": "extendAusDEM.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "shraem_aus_20000211_ac0g0.tif": {
                                                                                "filename": "shraem_aus_20000211_ac0g0.tif 2010-03-25 22:27:54",
                                                                                "metadata": {
                                                                                    "script": "insertDEMHistory.py"
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            }
                                                        ]
                                                    }
                                                },
                                                {
                                                    "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:26:21",
                                                        "metadata": {
                                                            "script": "qv_sentinel2angles.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:26:21",
                                                                    "metadata": {
                                                                        "script": ""
                                                                    },
                                                                    "input": []
                                                                }
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        }
                                    },
                                    {
                                        "cgmsre_t54hvh_20250211_aa3m4_zincidence.img": {
                                            "filename": "cgmsre_t54hvh_20250211_aa3m4_zincidence.img 2025-02-13 00:26:56",
                                            "metadata": {
                                                "script": "makeincidenceangles.py"
                                            },
                                            "input": [
                                                {
                                                    "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:26:21",
                                                        "metadata": {
                                                            "script": "qv_sentinel2angles.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:26:21",
                                                                    "metadata": {
                                                                        "script": ""
                                                                    },
                                                                    "input": []
                                                                }
                                                            }
                                                        ]
                                                    }
                                                },
                                                {
                                                    "cgmsre_t54hvh_20250211_ab1m4_zdem.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_ab1m4_zdem.img 2025-02-13 00:26:33",
                                                        "metadata": {
                                                            "script": "demlike.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif": {
                                                                    "filename": "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif 2014-05-01 16:58:59",
                                                                    "metadata": {
                                                                        "script": "extendAusDEM.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "shraem_aus_20000211_ac0g0.tif": {
                                                                                "filename": "shraem_aus_20000211_ac0g0.tif 2010-03-25 22:27:54",
                                                                                "metadata": {
                                                                                    "script": "insertDEMHistory.py"
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        }
                                    },
                                    {
                                        "cgmsre_t54hvh_20250211_aa1m4.img": {
                                            "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:26:21",
                                            "metadata": {
                                                "script": "qv_sentinel2angles.py"
                                            },
                                            "input": [
                                                {
                                                    "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:26:21",
                                                        "metadata": {
                                                            "script": ""
                                                        },
                                                        "input": []
                                                    }
                                                }
                                            ]
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "cgmsre_t54hvh_20250211_aa3m4_zdiffuseirradj.img": {
                                "filename": "cgmsre_t54hvh_20250211_aa3m4_zdiffuseirradj.img 2025-02-13 00:27:44",
                                "metadata": {
                                    "script": "adjustdiffuseirr.py"
                                },
                                "input": [
                                    {
                                        "cgmsre_t54hvh_20250211_aa3m4_zdirectirr.img": {
                                            "filename": "cgmsre_t54hvh_20250211_aa3m4_zdirectirr.img 2025-02-13 00:27:10",
                                            "metadata": {
                                                "script": "atmoscorrectsentinel2.py"
                                            },
                                            "input": [
                                                {
                                                    "cgmsre_t54hvh_20250211_aa3m4.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa3m4.img 2025-02-13 00:26:28",
                                                        "metadata": {
                                                            "script": "toaradiance_sen2.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "cgmsre_t54hvh_20250211_ab1m4.img": {
                                                                    "filename": "cgmsre_t54hvh_20250211_ab1m4.img 2025-02-13 00:11:51",
                                                                    "metadata": {
                                                                        "script": "qv_importsentinel2_l1c.py"
                                                                    },
                                                                    "input": []
                                                                }
                                                            },
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:26:21",
                                                                    "metadata": {
                                                                        "script": "qv_sentinel2angles.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                                "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:26:21",
                                                                                "metadata": {
                                                                                    "script": ""
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            }
                                                        ]
                                                    }
                                                },
                                                {
                                                    "/mnt/scratch_lustre/tmp/rs_testing/download/tmpmm6jt_f5.img": {
                                                        "filename": "/mnt/scratch_lustre/tmp/rs_testing/download/tmpmm6jt_f5.img 2025-02-13 00:26:58",
                                                        "metadata": {
                                                            "script": "atmoscorrectsentinel2.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif": {
                                                                    "filename": "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif 2014-05-01 16:58:59",
                                                                    "metadata": {
                                                                        "script": "extendAusDEM.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "shraem_aus_20000211_ac0g0.tif": {
                                                                                "filename": "shraem_aus_20000211_ac0g0.tif 2010-03-25 22:27:54",
                                                                                "metadata": {
                                                                                    "script": "insertDEMHistory.py"
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            }
                                                        ]
                                                    }
                                                },
                                                {
                                                    "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:26:21",
                                                        "metadata": {
                                                            "script": "qv_sentinel2angles.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:26:21",
                                                                    "metadata": {
                                                                        "script": ""
                                                                    },
                                                                    "input": []
                                                                }
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        }
                                    },
                                    {
                                        "cgmsre_t54hvh_20250211_aa3m4_zdiffuseirr.img": {
                                            "filename": "cgmsre_t54hvh_20250211_aa3m4_zdiffuseirr.img 2025-02-13 00:27:16",
                                            "metadata": {
                                                "script": "atmoscorrectsentinel2.py"
                                            },
                                            "input": [
                                                {
                                                    "cgmsre_t54hvh_20250211_aa3m4.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa3m4.img 2025-02-13 00:26:28",
                                                        "metadata": {
                                                            "script": "toaradiance_sen2.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "cgmsre_t54hvh_20250211_ab1m4.img": {
                                                                    "filename": "cgmsre_t54hvh_20250211_ab1m4.img 2025-02-13 00:11:51",
                                                                    "metadata": {
                                                                        "script": "qv_importsentinel2_l1c.py"
                                                                    },
                                                                    "input": []
                                                                }
                                                            },
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:26:21",
                                                                    "metadata": {
                                                                        "script": "qv_sentinel2angles.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                                "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:26:21",
                                                                                "metadata": {
                                                                                    "script": ""
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            }
                                                        ]
                                                    }
                                                },
                                                {
                                                    "/mnt/scratch_lustre/tmp/rs_testing/download/tmpmm6jt_f5.img": {
                                                        "filename": "/mnt/scratch_lustre/tmp/rs_testing/download/tmpmm6jt_f5.img 2025-02-13 00:26:58",
                                                        "metadata": {
                                                            "script": "atmoscorrectsentinel2.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif": {
                                                                    "filename": "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif 2014-05-01 16:58:59",
                                                                    "metadata": {
                                                                        "script": "extendAusDEM.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "shraem_aus_20000211_ac0g0.tif": {
                                                                                "filename": "shraem_aus_20000211_ac0g0.tif 2010-03-25 22:27:54",
                                                                                "metadata": {
                                                                                    "script": "insertDEMHistory.py"
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            }
                                                        ]
                                                    }
                                                },
                                                {
                                                    "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:26:21",
                                                        "metadata": {
                                                            "script": "qv_sentinel2angles.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:26:21",
                                                                    "metadata": {
                                                                        "script": ""
                                                                    },
                                                                    "input": []
                                                                }
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        }
                                    },
                                    {
                                        "cgmsre_t54hvh_20250211_aa3m4_zsfcrad.img": {
                                            "filename": "cgmsre_t54hvh_20250211_aa3m4_zsfcrad.img 2025-02-13 00:27:28",
                                            "metadata": {
                                                "script": "atmoscorrectsentinel2.py"
                                            },
                                            "input": [
                                                {
                                                    "cgmsre_t54hvh_20250211_aa3m4.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa3m4.img 2025-02-13 00:26:28",
                                                        "metadata": {
                                                            "script": "toaradiance_sen2.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "cgmsre_t54hvh_20250211_ab1m4.img": {
                                                                    "filename": "cgmsre_t54hvh_20250211_ab1m4.img 2025-02-13 00:11:51",
                                                                    "metadata": {
                                                                        "script": "qv_importsentinel2_l1c.py"
                                                                    },
                                                                    "input": []
                                                                }
                                                            },
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:26:21",
                                                                    "metadata": {
                                                                        "script": "qv_sentinel2angles.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                                "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:26:21",
                                                                                "metadata": {
                                                                                    "script": ""
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            }
                                                        ]
                                                    }
                                                },
                                                {
                                                    "/mnt/scratch_lustre/tmp/rs_testing/download/tmpmm6jt_f5.img": {
                                                        "filename": "/mnt/scratch_lustre/tmp/rs_testing/download/tmpmm6jt_f5.img 2025-02-13 00:26:58",
                                                        "metadata": {
                                                            "script": "atmoscorrectsentinel2.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif": {
                                                                    "filename": "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif 2014-05-01 16:58:59",
                                                                    "metadata": {
                                                                        "script": "extendAusDEM.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "shraem_aus_20000211_ac0g0.tif": {
                                                                                "filename": "shraem_aus_20000211_ac0g0.tif 2010-03-25 22:27:54",
                                                                                "metadata": {
                                                                                    "script": "insertDEMHistory.py"
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            }
                                                        ]
                                                    }
                                                },
                                                {
                                                    "cgmsre_t54hvh_20250211_aa3m4_zdirectirr.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa3m4_zdirectirr.img 2025-02-13 00:27:10",
                                                        "metadata": {
                                                            "script": "atmoscorrectsentinel2.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa3m4.img": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa3m4.img 2025-02-13 00:26:28",
                                                                    "metadata": {
                                                                        "script": "toaradiance_sen2.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "cgmsre_t54hvh_20250211_ab1m4.img": {
                                                                                "filename": "cgmsre_t54hvh_20250211_ab1m4.img 2025-02-13 00:11:51",
                                                                                "metadata": {
                                                                                    "script": "qv_importsentinel2_l1c.py"
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        },
                                                                        {
                                                                            "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                                                "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:26:21",
                                                                                "metadata": {
                                                                                    "script": "qv_sentinel2angles.py"
                                                                                },
                                                                                "input": [
                                                                                    {
                                                                                        "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                                            "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:26:21",
                                                                                            "metadata": {
                                                                                                "script": ""
                                                                                            },
                                                                                            "input": []
                                                                                        }
                                                                                    }
                                                                                ]
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            },
                                                            {
                                                                "/mnt/scratch_lustre/tmp/rs_testing/download/tmpmm6jt_f5.img": {
                                                                    "filename": "/mnt/scratch_lustre/tmp/rs_testing/download/tmpmm6jt_f5.img 2025-02-13 00:26:58",
                                                                    "metadata": {
                                                                        "script": "atmoscorrectsentinel2.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif": {
                                                                                "filename": "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif 2014-05-01 16:58:59",
                                                                                "metadata": {
                                                                                    "script": "extendAusDEM.py"
                                                                                },
                                                                                "input": [
                                                                                    {
                                                                                        "shraem_aus_20000211_ac0g0.tif": {
                                                                                            "filename": "shraem_aus_20000211_ac0g0.tif 2010-03-25 22:27:54",
                                                                                            "metadata": {
                                                                                                "script": "insertDEMHistory.py"
                                                                                            },
                                                                                            "input": []
                                                                                        }
                                                                                    }
                                                                                ]
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            },
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:26:21",
                                                                    "metadata": {
                                                                        "script": "qv_sentinel2angles.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                                "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:26:21",
                                                                                "metadata": {
                                                                                    "script": ""
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            }
                                                        ]
                                                    }
                                                },
                                                {
                                                    "cgmsre_t54hvh_20250211_aa3m4_zdiffuseirr.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa3m4_zdiffuseirr.img 2025-02-13 00:27:16",
                                                        "metadata": {
                                                            "script": "atmoscorrectsentinel2.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa3m4.img": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa3m4.img 2025-02-13 00:26:28",
                                                                    "metadata": {
                                                                        "script": "toaradiance_sen2.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "cgmsre_t54hvh_20250211_ab1m4.img": {
                                                                                "filename": "cgmsre_t54hvh_20250211_ab1m4.img 2025-02-13 00:11:51",
                                                                                "metadata": {
                                                                                    "script": "qv_importsentinel2_l1c.py"
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        },
                                                                        {
                                                                            "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                                                "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:26:21",
                                                                                "metadata": {
                                                                                    "script": "qv_sentinel2angles.py"
                                                                                },
                                                                                "input": [
                                                                                    {
                                                                                        "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                                            "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:26:21",
                                                                                            "metadata": {
                                                                                                "script": ""
                                                                                            },
                                                                                            "input": []
                                                                                        }
                                                                                    }
                                                                                ]
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            },
                                                            {
                                                                "/mnt/scratch_lustre/tmp/rs_testing/download/tmpmm6jt_f5.img": {
                                                                    "filename": "/mnt/scratch_lustre/tmp/rs_testing/download/tmpmm6jt_f5.img 2025-02-13 00:26:58",
                                                                    "metadata": {
                                                                        "script": "atmoscorrectsentinel2.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif": {
                                                                                "filename": "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif 2014-05-01 16:58:59",
                                                                                "metadata": {
                                                                                    "script": "extendAusDEM.py"
                                                                                },
                                                                                "input": [
                                                                                    {
                                                                                        "shraem_aus_20000211_ac0g0.tif": {
                                                                                            "filename": "shraem_aus_20000211_ac0g0.tif 2010-03-25 22:27:54",
                                                                                            "metadata": {
                                                                                                "script": "insertDEMHistory.py"
                                                                                            },
                                                                                            "input": []
                                                                                        }
                                                                                    }
                                                                                ]
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            },
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:26:21",
                                                                    "metadata": {
                                                                        "script": "qv_sentinel2angles.py"
                                                                    },
                                                                    "input": [
                                                                        {
                                                                            "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                                "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:26:21",
                                                                                "metadata": {
                                                                                    "script": ""
                                                                                },
                                                                                "input": []
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            }
                                                        ]
                                                    }
                                                },
                                                {
                                                    "cgmsre_t54hvh_20250211_aa1m4.img": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:26:21",
                                                        "metadata": {
                                                            "script": "qv_sentinel2angles.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                                    "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:26:21",
                                                                    "metadata": {
                                                                        "script": ""
                                                                    },
                                                                    "input": []
                                                                }
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        }
                                    },
                                    {
                                        "cgmsre_t54hvh_20250211_da5m4_zdiffuseirr.img": {
                                            "filename": "cgmsre_t54hvh_20250211_da5m4_zdiffuseirr.img 2025-02-13 00:27:37",
                                            "metadata": {
                                                "script": "adjustdiffuseirr.py"
                                            },
                                            "input": [
                                                {
                                                    "/mnt/project/refData/skyview/lztmre_auszone54_eall_da5m4.tif": {
                                                        "filename": "/mnt/project/refData/skyview/lztmre_auszone54_eall_da5m4.tif 2012-04-03 11:00:22",
                                                        "metadata": {
                                                            "script": "wholeZoneViewFactor.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "/apollo/data/images/geog/shraem_aus_20000211_ac0g0.tif": {
                                                                    "filename": "/apollo/data/images/geog/shraem_aus_20000211_ac0g0.tif 2010-03-25 22:27:54",
                                                                    "metadata": {
                                                                        "script": "insertDEMHistory.py"
                                                                    },
                                                                    "input": []
                                                                }
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "cgmsre_t54hvh_20250211_aa3m4_zincidence.img": {
                                "filename": "cgmsre_t54hvh_20250211_aa3m4_zincidence.img 2025-02-13 00:26:56",
                                "metadata": {
                                    "script": "makeincidenceangles.py"
                                },
                                "input": [
                                    {
                                        "cgmsre_t54hvh_20250211_aa1m4.img": {
                                            "filename": "cgmsre_t54hvh_20250211_aa1m4.img 2025-02-13 00:26:21",
                                            "metadata": {
                                                "script": "qv_sentinel2angles.py"
                                            },
                                            "input": [
                                                {
                                                    "cgmsre_t54hvh_20250211_aa0m4.meta": {
                                                        "filename": "cgmsre_t54hvh_20250211_aa0m4.meta 2025-02-13 00:26:21",
                                                        "metadata": {
                                                            "script": ""
                                                        },
                                                        "input": []
                                                    }
                                                }
                                            ]
                                        }
                                    },
                                    {
                                        "cgmsre_t54hvh_20250211_ab1m4_zdem.img": {
                                            "filename": "cgmsre_t54hvh_20250211_ab1m4_zdem.img 2025-02-13 00:26:33",
                                            "metadata": {
                                                "script": "demlike.py"
                                            },
                                            "input": [
                                                {
                                                    "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif": {
                                                        "filename": "/mnt/project/refData/DEM/shraem_aus_20000211_ac0g0_zextended.tif 2014-05-01 16:58:59",
                                                        "metadata": {
                                                            "script": "extendAusDEM.py"
                                                        },
                                                        "input": [
                                                            {
                                                                "shraem_aus_20000211_ac0g0.tif": {
                                                                    "filename": "shraem_aus_20000211_ac0g0.tif 2010-03-25 22:27:54",
                                                                    "metadata": {
                                                                        "script": "insertDEMHistory.py"
                                                                    },
                                                                    "input": []
                                                                }
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "/mnt/appsource/local/rs/rsc/brdf/paramfiles/Sentinel2MSI20m.hsdref": {
                                "filename": "/mnt/appsource/local/rs/rsc/brdf/paramfiles/Sentinel2MSI20m.hsdref 2016-02-06 13:10:29",
                                "metadata": {
                                    "script": "calcHemisphericDirectionalRef.py"
                                },
                                "input": [
                                    {
                                        "Sentinel2MSI20m.rtlsparams": {
                                            "filename": "Sentinel2MSI20m.rtlsparams 2016-02-06 13:09:15",
                                            "metadata": {
                                                "script": " "
                                            },
                                            "input": [
                                                {
                                                    "SPOT_HRG.rtlsparams": {
                                                        "filename": "SPOT_HRG.rtlsparams 2011-08-05 10:56:28",
                                                        "metadata": {
                                                            "script": "fit_brdf.py"
                                                        },
                                                        "input": []
                                                    }
                                                },
                                                {
                                                    "LandsatTM.rtlsparams": {
                                                        "filename": "LandsatTM.rtlsparams 2011-04-14 15:24:36",
                                                        "metadata": {
                                                            "script": "fit_brdf.py"
                                                        },
                                                        "input": []
                                                    }
                                                }
                                            ]
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                }
            }
        ]
    }
}


# Extract TABLES and steps
TABLES, processing_steps = parse_json_data(json_data)

# Ensure OpenMetadata entities are set up
ensure_service_exists()
ensure_database_exists()
ensure_schema_exists()

# Ensure all tables exist in OpenMetadata
for file_path, table_fqn in TABLES.items():
    table_name = table_fqn.split(".")[-2]
    ensure_table_exists(table_name, table_fqn)

# DAG default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=1),
}

# Define DAG
with DAG(
    "test_sentinel2_json_full_lineage_3",
    default_args=default_args,
    description="Dynamically generated Sentinel-2 pipeline from JSON with lineage",
    start_date=days_ago(1),
    is_paused_upon_creation=False,
    catchup=False,
) as dag:

# Create tasks dynamically
    task_map = {}

    # Parsing metadata task (no inlets/outlets, as in example)
    parsing_metadata = DummyOperator(task_id="parsing_metadata")
    task_map["parsing_metadata"] = {"output": parsing_metadata}

    # Process each script-based stage
    for script_id, details in processing_steps.items():

        if script_id == " ":
            script_id = "rtlsparams"

        input_task = DummyOperator(
            task_id=f"{script_id}_input",
            inlets=[OMEntity(entity=Table, fqn=fqn, key=script_id) for fqn in details["inputs"].union(details["raw_inputs"])],
        )
        output_task = DummyOperator(
            task_id=f"{script_id}_output",
            outlets=[OMEntity(entity=Table, fqn=fqn, key=script_id) for fqn in details["outputs"]],
        )
        input_task >> output_task
        task_map[script_id] = {"input": input_task, "output": output_task}

    # Define dependencies (mimicking the example structure)
    # parsing_metadata -> qv-sentinel2angles
    parsing_metadata >> task_map["qv-sentinel2angles"]["input"]

    # qv-sentinel2angles -> [toaradiance-sen2, makeincidenceangles] (two parallel paths)
    task_map["qv-sentinel2angles"]["output"] >> [
        task_map["toaradiance-sen2"]["input"],
        task_map["makeincidenceangles"]["input"]
    ]

    # Path 1: ab0 processing
    task_map["toaradiance-sen2"]["output"] >> task_map["atmoscorrectsentinel2"]["input"]
    task_map["atmoscorrectsentinel2"]["output"] >> task_map["computedirectirr"]["input"]
    task_map["atmoscorrectsentinel2"]["output"] >> task_map["adjustdiffuseirr"]["input"]
    task_map["computedirectirr"]["output"] >> task_map["standardizereflectance"]["input"]
    task_map["adjustdiffuseirr"]["output"] >> task_map["standardizereflectance"]["input"]
    task_map["makeincidenceangles"]["output"] >> task_map["standardizereflectance"]["input"]

    # Path 2: ab1 processing (simplified, assuming similar scripts)
    # Note: JSON has two toaradiance instances; we'll assume one per path for clarity
    task_map["qv-sentinel2angles"]["output"] >> task_map["toaradiance-sen2"]["input"]  # Reused for ab1 path
    task_map["toaradiance-sen2"]["output"] >> task_map["atmoscorrectsentinel2"]["input"]
    task_map["atmoscorrectsentinel2"]["output"] >> task_map["computedirectirr"]["input"]
    task_map["atmoscorrectsentinel2"]["output"] >> task_map["adjustdiffuseirr"]["input"]
    task_map["computedirectirr"]["output"] >> task_map["standardizereflectance"]["input"]
    task_map["adjustdiffuseirr"]["output"] >> task_map["standardizereflectance"]["input"]
    task_map["makeincidenceangles"]["output"] >> task_map["standardizereflectance"]["input"]

    # Convergence at water index
    task_map["standardizereflectance"]["output"] >> task_map["qv-water-index2015"]["input"]

