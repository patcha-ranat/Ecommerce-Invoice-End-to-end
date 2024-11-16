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

from datetime import datetime
from airflow import models
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator


DAG_ID: str = "kde.data_product.ml_dynamic_segment"

PROJECT_ID: str = models.Variable.get("project_id")

default_config: dict = {
    "image": "interpretable-dynamic-rfm-service",
    "image_version": "2"
}

IMAGE_ID: str = f"{default_config.get('image')}:v{default_config.get('image_version')}"

gcs: dict = models.Variable.get("gcs")

default_dag_args: dict = {
    "doc_md": __doc__,
    "default_view": "grid",
    "retries": 2,
    "catchup": False
}

with models.DAG(
        dag_id=DAG_ID,
        start_date=datetime(2024, 11, 1),
        schedule_interval="0 14 * * *",
        default_args=default_dag_args,
        tags=["data_product", "ml"]
) as dag:
    start_task = EmptyOperator(task_id="start")

    segment_task = DockerOperator(
        task_id="segment",
        image=IMAGE_ID,
        # mounts=,
        auto_remove=True,
        command=[
            "--env", "gcp", 
            "--project_id", PROJECT_ID,
            "--method", "filesystem", 
            "--input_path", f"gs://{gcs.get('landing')}/input/data/{{ ds }}/ecomm_invoice_transaction.parquet", 
            "--output_path", f"gs://{gcs.get('staging')}/output", 
            "--exec_date", "{{ ds }}"
        ]
    )

    end_task = EmptyOperator(task_id="end")

    # DAG Dependency
    start_task >> segment_task >> end_task