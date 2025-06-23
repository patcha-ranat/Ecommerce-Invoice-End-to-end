"""
"""

from datetime import datetime, timedelta

from airflow import models


from data_ingestion.pipeline.dataproc import dataproc_serverless_pipeline, dataproc_cluster_pipeline
from data_ingestion.config.config import table_configs


# DAG Attributes
ORGANIZATION = "kde"
DAG_TYPE = "data_ingestion"
CLOUD = "gcp"

# airflow variables
PROJECT_ID: str = models.Variable.get("project_id")
REGION: str = models.Variable.get("region")
GCS: dict = models.Variable.get("gcs", deserialize_json=True)
GCP_SERVICE_ACCOUNT: str = models.Variable.get("gcp_service_account")

# DAG configuration
default_config: dict = {
    "main_python_file_uri": "__main__.py",
    "py_zip_filename": "kde_ecomm_ingestion.zip",
    "jar_file": "gcs-connector-3.0.7-shaded.jar",
    "dag_args": {
        "ecomm_transactions": {
            "start_date": datetime(2025, 5, 12),
            "schedule": "0 1 * * *",
            "catchup": False,
            "tags": ["data_ingestion", "gcp", "pyspark"]
        }
    },
    "cluster_name": "dev-ingestion",
    "cluster_config": {
        "gce_cluster_config": {
            "service_account": GCP_SERVICE_ACCOUNT,
        },
        "master_config": {
            "num_instances": 1,
            "machine_type_uri": "n1-standard-2", # lowest spec 'n1-standard-2' is not supported
            "disk_config": {
                "boot_disk_type": "pd-standard", 
                "boot_disk_size_gb": 32 # min 30
            },
        },
        "worker_config": {
            "num_instances": 2,
            "machine_type_uri": "n1-standard-2",
            "disk_config": {
                "boot_disk_type": "pd-standard", 
                "boot_disk_size_gb": 32
            },
        },
        "secondary_worker_config": {
            "num_instances": 1,
            "machine_type_uri": "n1-standard-2",
            "disk_config": {
                "boot_disk_type": "pd-standard",
                "boot_disk_size_gb": 32,
            },
            "is_preemptible": True,
            "preemptibility": "PREEMPTIBLE",
        }
    }
}

# DAG Arguments
default_dag_args: dict = {
    "doc_md": __doc__,
    "default_view": "grid",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False
}

# Serverless
for ingestion_config in table_configs:
    table_name = ingestion_config.get("table_name")
    dag_id: str = f"{ORGANIZATION}.{DAG_TYPE}.{CLOUD}_{table_name}_dataproc_batch"
    
    # overriding dag config depending on environment
    # dag config is the configuration that will be used within a pipeline
    dag_config = {**default_config, **models.Variable.get(dag_id, {})}
    # dag args (arguments) are dag's attributes
    dag_args: dict = default_config.get("dag_args").get(table_name)

    # initialize dag with pipeline
    globals()[dag_id] = dataproc_serverless_pipeline(
        dag_id=dag_id,
        dag_args=dag_args,
        default_dag_args=default_dag_args,
        project_id=PROJECT_ID,
        region=REGION,
        dag_config=dag_config,
        gcs=GCS,
        gcp_service_account=GCP_SERVICE_ACCOUNT,
        ingestion_config=ingestion_config
    )

# Cluster
for ingestion_config in table_configs:
    table_name = ingestion_config.get("table_name")
    dag_id: str = f"{ORGANIZATION}.{DAG_TYPE}.{CLOUD}_{table_name}_dataproc_cluster"
    
    # overriding dag config depending on environment
    # dag config is the configuration that will be used within a pipeline
    dag_config = {**default_config, **models.Variable.get(dag_id, {})}
    # dag args (arguments) are dag's attributes
    dag_args: dict = default_config.get("dag_args").get(table_name)

    # initialize dag with pipeline
    globals()[dag_id] = dataproc_cluster_pipeline(
        dag_id=dag_id,
        dag_args=dag_args,
        default_dag_args=default_dag_args,
        project_id=PROJECT_ID,
        region=REGION,
        dag_config=dag_config,
        gcs=GCS,
        ingestion_config=ingestion_config,
    )