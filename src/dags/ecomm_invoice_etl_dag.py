from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from datetime import datetime
from airflow.utils import timezone
from datetime import timedelta
import requests
from kaggle.api.kaggle_api_extended import KaggleApi
import os
import tempfile
import shutil
from google.cloud import storage
import logging

from transform_load import clean_data, load_data, clear_staging_area

# get variables from .env file
project_id = os.environ["PROJECT_ID"]
bucket_name = os.environ["BUCKET_NAME"]
dataset_name = os.environ["DATASET_NAME"]
table_name = os.environ["TABLE_NAME"]
credentials_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
load_target_file = "ecomm_invoice_transaction.parquet"


def _extract_data_from_url():
    # retrieve data from url
    url = "https://raw.githubusercontent.com/Patcharanat/ecommerce-invoice/master/data/cleaned_data.csv"
    response = requests.get(url)
    data_url = response.text

    # specify desired file name and credentials path
    destination_blob_name = "data_url_uncleaned.csv"

    # write data to csv file created in GCS
    storage_client = storage.Client.from_service_account_json(credentials_path)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    with blob.open("w") as file:
        file.write(data_url)
    file.close()
    logging.info(f"File {destination_blob_name} is stored.")


def _extract_data_from_database():
    # initiate connection
    postgres_hook = PostgresHook(
        postgres_conn_id="postgres-local",
        schema="mydatabase"
    )
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Define the SQL query to extract data
    query = "SELECT * FROM ecomm_invoice"

    # Define the COPY command with the query, CSV format, and headers
    copy_command = f"COPY ({query}) TO STDOUT WITH CSV HEADER"

    # specify desired file name and credentials path
    destination_blob_name = "data_postgres_cleaned.csv"

    # upload to GCS
    storage_client = storage.Client.from_service_account_json(credentials_path)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Open the file in write mode
    with blob.open('w') as file:
        # Execute the COPY command and write the results to the file
        cursor.copy_expert(copy_command, file)
    
    # close cursor and connection
    cursor.close()
    conn.close()


def _extract_data_from_api():
    # authenticate and download data from Kaggle API
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files('carrie1/ecommerce-data', path='./data/')

    # Extract the ZIP file to the temporary directory
    temp_dir = tempfile.mkdtemp()  # Create a temporary directory
    zip_path = './data/ecommerce-data.zip' # destination path
    shutil.unpack_archive(zip_path, temp_dir, format='zip')
    extracted_file_path = os.path.join(temp_dir, 'data.csv')

    # upload to GCS
    destination_blob_name = "data_api_uncleaned.csv"

    storage_client = storage.Client.from_service_account_json(credentials_path)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(extracted_file_path)

    # remove created csv file from local
    os.remove(zip_path)


default_args = {
    "owner": "Ken",
    # "email": ["XXXXX"],
    "start_date": timezone.datetime(2023, 6, 26),
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    'ecomm_invoice_etl_dag',
    start_date=datetime(2023, 5, 30), 
    schedule_interval=None # or '@daily'
    ) as dag:
    
    extract_data_from_url = PythonOperator(
        task_id='extract_data_from_url',
        python_callable=_extract_data_from_url
    )

    extract_data_from_database = PythonOperator(
        task_id='extract_data_from_database',
        python_callable=_extract_data_from_database
    )

    extract_data_from_api = PythonOperator(
        task_id='extract_data_from_api',
        python_callable=_extract_data_from_api
    )

    transform_data_from_data_lake = PythonOperator(
        task_id="transform_data_from_data_lake",
        python_callable=clean_data,
        op_kwargs={
            "source_file": "data_api_uncleaned.csv",
            "destination_file": load_target_file,
            "project_id": project_id,
            "bucket_name": bucket_name,
            "credentials_path": credentials_path
        }
    )

    load_to_bigquery = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_data,
        op_kwargs={
            "project_id": project_id,
            "bucket_name": bucket_name,
            "dataset_name": dataset_name,
            "table_name": table_name,
            "credentials_path": credentials_path
        }
    )

    load_to_bigquery_external = BigQueryCreateExternalTableOperator(
        task_id="load_to_bigquery_external",
        table_resource={
            "tableReference": {
                "projectId": project_id,
                "datasetId": dataset_name,
                "tableId": f"{table_name}_external",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{bucket_name}/staging_area/{load_target_file}"],
            },
        },
        gcp_conn_id="my_gcp_conn_id",
    )

    # clear_staging_area_gcs = PythonOperator(
    #     task_id="clear_staging_area_gcs",
    #     python_callable=clear_staging_area,
    #     op_kwargs={
    #         "bucket_name": bucket_name,
    #         "blob_name": f"staging_area/{load_target_file}",
    #         "credentials_path": credentials_path
    #     }
    # )

    [extract_data_from_api, extract_data_from_database, extract_data_from_url] >> transform_data_from_data_lake
    transform_data_from_data_lake >> [load_to_bigquery, load_to_bigquery_external] 
    # [load_to_bigquery, load_to_bigquery_external] >> clear_staging_area_gcs
    