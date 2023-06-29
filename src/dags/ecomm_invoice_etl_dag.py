from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from airflow.utils import timezone
from datetime import timedelta
import requests
from kaggle.api.kaggle_api_extended import KaggleApi
import os
import tempfile
import shutil
from google.cloud import storage
import pyarrow.csv as pv
import pyarrow.parquet as pq
import logging
import gcsfs

project_id = os.environ["PROJECT_ID"]
bucket_name = os.environ["BUCKET_NAME"]
credentials_path = '/opt/airflow/gcs_credentials.json'

def transform_csv_to_parquet(destination_blob_name, credentials) -> None:
    """
    transform csv to parquet in data lake
    """
    csv_file_path = f"gs://ecomm-invoice-data-lake-bucket/{destination_blob_name}"
    parquet_file_path = destination_blob_name.replace("csv", "parquet")

    # Open GCS CSV file using gcsfs
    fs = gcsfs.GCSFileSystem(project=project_id, token=credentials)
    with fs.open(csv_file_path, 'rb') as file:
        # Read CSV file using pyarrow.csv with explicit encoding
        options = pv.ReadOptions(encoding='cp1252')
        csv_table = pv.read_csv(file, options)

    pq.write_table(csv_table, parquet_file_path)
    logging.info(f"File {destination_blob_name} is converted to parquet as {parquet_file_path}.")


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

    # transform csv to parquet in data lake
    # transform_csv_to_parquet(destination_blob_name, credentials=storage_client._credentials)


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

    # transform csv to parquet in data lake
    # transform_csv_to_parquet(destination_blob_name, credentials=storage_client._credentials)
    

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

    # transform csv to parquet in data lake
    # transform_csv_to_parquet(destination_blob_name, credentials=storage_client._credentials)

def _transform_data_from_data_lake():
    pass

def _load_to_data_warehouse():
    pass

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
        python_callable=_extract_data_from_api,
    )

    transform_data_from_data_lake = DummyOperator(
        task_id="transform_data_from_data_lake"
    )

    extract_data_from_api >> transform_data_from_data_lake
    extract_data_from_database >> transform_data_from_data_lake
    extract_data_from_url >> transform_data_from_data_lake