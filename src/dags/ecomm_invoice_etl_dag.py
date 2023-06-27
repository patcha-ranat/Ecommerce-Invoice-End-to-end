from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from airflow.utils import timezone
from datetime import timedelta
import requests
from kaggle.api.kaggle_api_extended import KaggleApi
import os
import json
import tempfile
import shutil
from google.cloud import storage
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

bucket_name = "ecomm-invoice-data-lake-bucket"

def _extract_data_from_url():

    url = "https://raw.githubusercontent.com/Patcharanat/ecommerce-invoice/master/data/cleaned_data.csv"
    response = requests.get(url)
    data_url = response.text

    df = pd.read_csv(pd.compat.StringIO(data_url), delimiter='\t')
    table = pa.Table.from_pandas(df)
    pq.write_table(table=table, where='data_url_uncleaned.parquet')

    source_file_name = "data_url_uncleaned.parquet"
    destination_blob_name = "data_url_uncleaned.parquet"
    credentials_path = '../gcs_credentials.json'

    storage_client = storage.Client.from_service_account_json(credentials_path)
    bucket = storage_client.get_bucket(bucket_name)
    # bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )



def _extract_data_from_database():
    # initiate connection
    postgres_hook = PostgresHook(
        postgres_conn_id="local-postgres",
        schema="myschema"
    )
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Define the SQL query to extract data
    query = "SELECT * FROM ecomm_invoice"

    # Create a PostgresOperator to execute the query
    cursor.execute(query)

    # Fetch the data
    data_database = cursor.fetchall()
    # data_database = PostgresOperator(
    #     task_id='extract_data_from_database',
    #     postgres_conn_id='postgres-local',
    #     sql=query
    # )

    # s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    # s3_hook.load_string(
    #     string_data=data_database.output,
    #     key='data_database_cleaned.csv',
    #     bucket_name=bucket_name,
    #     replace=True
    # )

def _extract_data_from_api():
    target_file = open("kaggle.json")

    data = json.load(target_file)
    os.environ['KAGGLE_USERNAME'] = data['username']
    os.environ['KAGGLE_KEY'] = data['key']
    target_file.close()

    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files('carrie1/ecommerce-data', path='./data/')

    temp_dir = tempfile.mkdtemp()  # Create a temporary directory
    zip_path = './data/ecommerce-data.zip'

    # Extract the ZIP file to the temporary directory
    shutil.unpack_archive(zip_path, temp_dir, format='zip')

    extracted_file_path = os.path.join(temp_dir, 'data.csv')

    # with open(extracted_file_path, 'r', encoding='cp1252') as infile:
    #     data_api = infile.read()

    # s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    # s3_hook.load_file(
    #     extracted_file_path,
    #     key='data_api_uncleaned.csv',
    #     bucket_name=bucket_name,
    #     replace=True
    # )

def _transform_data_from_data_lake():
    pass

def _load_to_data_warehouse():
    pass

default_args = {
    "owner": "Patcharanat",
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

    # extract_data_from_database = PythonOperator(
    #     task_id='extract_data_from_database',
    #     python_callable=_extract_data_from_database
    # )

    # extract_data_from_api = PythonOperator(
    #     task_id='extract_data_from_api',
    #     python_callable=_extract_data_from_api,
    # )

    # Define dependencies