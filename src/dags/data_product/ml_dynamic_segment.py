"""
# ML Dynamic Segmentation DAG

Call Docker Image from GAR and execute with airflow parameter
1. Read Input
2. Create RFM DataFrame
3. Feed RFM DataFrame to ML Services
    - Scale
    - Automated Clustering
    - Interpret Cluster
4. Write Output
"""

import os
from datetime import datetime, timedelta
from airflow import models
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


# DAG Attributes
DAG_ID: str = "kde.data_product.ml_dynamic_segment"

# airflow variables
PROJECT_ID: str = models.Variable.get("project_id")
REGION: str = models.Variable.get("region")
GAR_REPO: str = models.Variable.get("gar_repo")
GCS: dict = models.Variable.get("gcs", deserialize_json=True)

# DAG Config
default_config: dict = {
    "image": "ecomm/interpretable-dynamic-rfm-service:v5"
}

# tasks config
image_id = f"{REGION}-docker.pkg.dev/{PROJECT_ID}/{GAR_REPO}/{default_config.get('image')}"
data_file_name = "ecomm_invoice_transaction.parquet"
execution_date = '{{ ds }}'
# Mount GCP ADC
localhost_path = "/run/desktop/mnt/host/c/Users/HP/AppData/Roaming/gcloud" # change this to match tester's host OS
docker_in_docker_path = "/root/.config/gcloud"


default_dag_args: dict = {
    "doc_md": __doc__,
    "default_view": "grid",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False
}

with models.DAG(
        dag_id=DAG_ID,
        start_date=datetime(2024, 11, 1),
        schedule="0 14 * * *",
        catchup=False,
        default_args=default_dag_args,
        tags=["data_product", "ml"]
) as dag:
    start_task = EmptyOperator(task_id="start")

    ml_segment_service_task = DockerOperator(
        task_id="ml_segment_service",
        docker_url="unix://var/run/docker.sock",
        image=image_id,
        environment={
            "GOOGLE_CLOUD_PROJECT": PROJECT_ID
        },
        mounts=[
            Mount(
                source=localhost_path,
                target=docker_in_docker_path,
                type="bind"
            )
        ],
        mount_tmp_dir=False,
        auto_remove="force",
        command=[
            "--env", "gcp", 
            "--project_id", PROJECT_ID,
            "--method", "filesystem", 
            "--input_path", f"gs://{GCS.get('landing')}/input/data/{execution_date}/{data_file_name}", 
            "--output_path", f"gs://{GCS.get('staging')}/output", 
            "--exec_date", execution_date
        ]
    )

    # Test: Keep DockerOperator's container run, to be able to check mounted directories
    # segment_task = DockerOperator(
    #     task_id="ml_service_debug_container",
    #     docker_url="unix://var/run/docker.sock",
    #     image=image_id,
    #     timeout=300,
    #     auto_remove="never",
    #     entrypoint=["tail", "-f", "/dev/null"],
    #     command=[],
    #     mounts=[
    #         Mount(
    #             source="/",
    #             target="/test_mount",
    #             type="bind"
    #         )
    #     ],
    #     mount_tmp_dir=False,
    # )

    end_task = EmptyOperator(task_id="end")

    # DAG Dependency
    start_task >> ml_segment_service_task >> end_task
