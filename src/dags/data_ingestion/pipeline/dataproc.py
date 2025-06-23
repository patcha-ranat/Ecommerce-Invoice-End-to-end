import logging

from airflow import models

# from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup

from airflow.operators.empty import EmptyOperator

# cluster
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocStartClusterOperator,
    DataprocSubmitJobOperator,
    DataprocStopClusterOperator,
    DataprocDeleteClusterOperator
)

# serverless
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator


def dataproc_serverless_pipeline(
    dag_id: str,
    dag_args: dict,
    default_dag_args: dict,
    project_id: str,
    region: str,
    dag_config: dict,
    gcs: dict[str, str],
    gcp_service_account: str,
    ingestion_config: dict
):
    """Batch Job"""
    # Variables
    MAIN_PYTHON_FILE_URI = dag_config.get("main_python_file_uri")
    PYTHON_FILE_URIS = [f"gs://{gcs.get('artifact')}/{dag_config.get('py_zip_filename')}"]
    INPUT_PATH = f"gs://{gcs.get('landing')}/ecomm_transaction/{{{{ ds }}}}/uncleaned_data.csv"
    OUTPUT_PATH = f"gs://{gcs.get('staging')}/ecomm_transaction/{{{{ ds }}}}/ecomm_transaction.parquet"
    JAR_FILE_URIS = [f"gs://{gcs.get('artifact')}/{dag_config.get('jar_file')}"]
    BATCH_ID = f"gcp-kde-ecomm-ingestion-{{{{ ds }}}}"
    STRING_INGESTION_CONFIG = str(ingestion_config)

    # DAGs
    with models.DAG(
        dag_id=dag_id,
        start_date=dag_args.get("start_date"),
        schedule=dag_args.get("schedule"),
        catchup=dag_args.get("catchup"),
        default_args=default_dag_args,
        tags=dag_args.get("tags")
    ) as dag:
        
        start_task = EmptyOperator(task_id="start")

        # Using Dataproc Serverless (Batch)
        dataproc_ingestion_task = DataprocCreateBatchOperator(
            task_id="dataproc_pyspark_ingestion_ecomm_transaction",
            
            # Mandatory
            region=region,
            batch={
                "pyspark_batch": {
                    "main_python_file_uri": MAIN_PYTHON_FILE_URI, # supported only `.py`
                    "python_file_uris": PYTHON_FILE_URIS, # supported `.py`, `.egg`, `.zip`
                    "args": [
                        "--spark-log-level", "ERROR",
                        "--ingestion-config", STRING_INGESTION_CONFIG,
                        "--input-path", INPUT_PATH,
                        "--output-path", OUTPUT_PATH,
                        "--dt", "{{ ds }}",
                        "--write-mode", "overwrite",
                        "--read-format", "csv",
                        "--write-format", "parquet",
                        "--ingestion-mode", "batch",
                    ],
                    "jar_file_uris": JAR_FILE_URIS
                },
                "runtime_config": {"version": "2.1"},
                "environment_config": {
                    "execution_config": {
                        "service_account": gcp_service_account,
                    }
                }
            },
            batch_id=BATCH_ID,

            # Optional
            project_id=project_id,
            num_retries_if_resource_is_not_ready=2,
            metadata=[("project", "kde_ecomm"), ("module", "pyspark_ingestion_framework")],
            # gcp_conn_id= # leave blank to let the operator use default google connection (ADC)
            polling_interval_seconds=10
        )

        # create_external_table_task = 

        end_task = EmptyOperator(task_id="end")

        # DAG Dependency
        start_task >> dataproc_ingestion_task >> end_task

        return dag

def dataproc_cluster_pipeline(
    dag_id: str,
    dag_args: dict,
    default_dag_args: dict,
    project_id: str,
    region: str,
    dag_config: dict,
    gcs: dict[str, str],
    ingestion_config: str,
):
    """Cluster Job"""
    # Variables
    MAIN_PYTHON_FILE_URI = dag_config.get("main_python_file_uri")
    PYTHON_FILE_URIS = [f"gs://{gcs.get('artifact')}/{dag_config.get('py_zip_filename')}"]
    INPUT_PATH = f"gs://{gcs.get('landing')}/ecomm_transaction/{{{{ ds }}}}/uncleaned_data.csv"
    OUTPUT_PATH = f"gs://{gcs.get('staging')}/ecomm_transaction/{{{{ ds }}}}/ecomm_transaction.parquet"
    JAR_FILE_URIS = [f"gs://{gcs.get('artifact')}/{dag_config.get('jar_file')}"]
    STRING_INGESTION_CONFIG = str(ingestion_config)

    CLUSTER_NAME = dag_config.get("cluster_name")
    CLUSTER_CONFIG = dag_config.get("cluster_config")

    PYSPARK_JOB = {
        "pyspark_job": {
            "main_python_file_uri": MAIN_PYTHON_FILE_URI, # driver script name
            "args": [
                "--spark-log-level", "ERROR",
                "--ingestion-config", STRING_INGESTION_CONFIG,
                "--input-path", INPUT_PATH,
                "--output-path", OUTPUT_PATH,
                "--dt", "{{ ds }}",
                "--write-mode", "overwrite",
                "--read-format", "csv",
                "--write-format", "parquet",
                "--ingestion-mode", "batch",
            ],
            "python_file_uris": PYTHON_FILE_URIS, # .py, .egg, .zip
            "jar_file_uris": [JAR_FILE_URIS]
        }
    }

    own_specified_connection_name = "google-cloud-default"

    # DAG
    with models.DAG(
        dag_id=dag_id,
        start_date=dag_args.get("start_date"),
        schedule=dag_args.get("schedule"),
        catchup=dag_args.get("catchup"),
        default_args=default_dag_args,
        tags=dag_args.get("tags")
    ) as dag:
        
        start_task = EmptyOperator(task_id="start")

        # Using Dataproc Cluster
        with TaskGroup(group_id="dataproc_ingestion") as dataproc_ingestion_task:
            start_ingestion_task = EmptyOperator(task_id="start_ingestion")
            
            create_cluster_task = DataprocCreateClusterOperator(
                task_id=f"create_{CLUSTER_NAME}_cluster",
                project_id=project_id,
                cluster_config=CLUSTER_CONFIG,
                region=region,
                cluster_name=CLUSTER_NAME,
                gcp_conn_id=own_specified_connection_name,
            )

            start_cluster_task = DataprocStartClusterOperator(
                task_id=f"start_{CLUSTER_NAME}_cluster",
                project_id=project_id,
                region=region,
                cluster_name=CLUSTER_NAME,
                gcp_conn_id=own_specified_connection_name,
            )

            submit_job_task = DataprocSubmitJobOperator(
                task_id=f"submit_pyspark_job_{CLUSTER_NAME}_cluster", 
                job=PYSPARK_JOB,
                region=region,
                project_id=project_id,
                gcp_conn_id=own_specified_connection_name,
            )

            stop_cluster_task = DataprocStopClusterOperator(
                task_id=f"stop_{CLUSTER_NAME}_cluster",
                project_id=project_id,
                region=region,
                cluster_name=CLUSTER_NAME,
                gcp_conn_id=own_specified_connection_name,
            )

            delete_cluster_task = DataprocDeleteClusterOperator(
                task_id=f"delete_{CLUSTER_NAME}_cluster",
                project_id=project_id,
                cluster_name=CLUSTER_NAME,
                region=region,
                gcp_conn_id=own_specified_connection_name,
                trigger_rule="all_done"
            )

            end_ingestion_task = EmptyOperator(task_id="end_ingestion")

            # Taskgroup Dependency
            (
                start_ingestion_task >> 
                create_cluster_task >> 
                start_cluster_task >> 
                submit_job_task >> 
                stop_cluster_task >> 
                delete_cluster_task >> 
                end_ingestion_task
            )

        # create_external_table_task = 

        end_task = EmptyOperator(task_id="end")

        # DAG Dependency
        start_task >> dataproc_ingestion_task >> end_task

        return dag
