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

from datetime import datetime, timedelta
from airflow import models
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator


# DAG Attributes
DAG_ID: str = "kde.data_product.ml_dynamic_segment"

# airflow variables
PROJECT_ID: str = models.Variable.get("project_id")
REGION: str = models.Variable.get("region")
GAR_REPO: str = models.Variable.get("gar_repo")
GCS: dict = models.Variable.get("gcs", deserialize_json=True)

# DAG Config
default_config: dict = {
    "image": "interpretable-dynamic-rfm-service:v2"
}

image_id = f"{REGION}-docker.pkg.dev/{PROJECT_ID}/{GAR_REPO}/{default_config.get('image')}"


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

    segment_task = DockerOperator(
        task_id="segment",
        image=image_id,
        # mounts=,
        auto_remove=True,
        command=[
            "--env", "gcp", 
            "--project_id", PROJECT_ID,
            "--method", "filesystem", 
            "--input_path", f"gs://{GCS.get('landing')}/input/data/{{{{ ds }}}}/ecomm_invoice_transaction.parquet", 
            "--output_path", f"gs://{GCS.get('staging')}/output", 
            "--exec_date", "{{ ds }}"
        ]
    )

    end_task = EmptyOperator(task_id="end")

    # DAG Dependency
    start_task >> segment_task >> end_task
