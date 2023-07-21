from kaggle.api.kaggle_api_extended import KaggleApi
import os
import shutil
import tempfile
import boto3
import pandas as pd
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
import s3fs
import logging

aws_credentials_path = os.environ["AWS_CREDENTIALS_PATH"]
aws_bucket = os.environ["AWS_BUCKET"]

def extract_url_aws():
    url = "https://raw.githubusercontent.com/Patcharanat/ecommerce-invoice/master/data/cleaned_data.csv"
    response = requests.get(url)
    data_url = response.text

    # retrieve credentials
    key = pd.read_csv(aws_credentials_path)
    _aws_access_key_id = key["Access key ID"][0]
    _aws_secret_access_key = key["Secret access key"][0]

    # authenticate and upload to S3
    # session = boto3.Session(
    #     aws_access_key_id = _aws_access_key_id,
    #     aws_secret_access_key = _aws_secret_access_key
    # )

    # s3 = session.client('s3')

    object_name = "data_url_uncleaned.csv"

    s3 = boto3.client('s3',
                      aws_access_key_id=_aws_access_key_id,
                      aws_secret_access_key=_aws_secret_access_key)
    s3.put_object(Bucket=aws_bucket, Key=object_name, Body=data_url)
    logging.info(f"File {object_name} is stored.")


def extract_database_aws():
    # initiate connection
    postgres_hook = PostgresHook(
        postgres_conn_id="postgres-local",
        schema="mydatabase"
    )
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Define the SQL query to extract data
    query = "SELECT * FROM ecomm_invoice"

    # specify filename
    csv_filename = "unloaded_data.csv"

    # Define the COPY command with the query, CSV format, and headers
    cursor.execute(f"COPY ({query}) TO {csv_filename} WITH CSV HEADER")

    # close cursor and connection
    cursor.close()
    conn.close()

    # specify desired target file name
    object_name = "data_postgres_cleaned.csv"

    # retrieve credentials
    key = pd.read_csv(aws_credentials_path)
    _aws_access_key_id = key["Access key ID"][0]
    _aws_secret_access_key = key["Secret access key"][0]

    # authenticate and upload to S3
    s3 = boto3.client('s3',
                        aws_access_key_id=_aws_access_key_id,
                        aws_secret_access_key=_aws_secret_access_key)
    
    with open(csv_filename, "rb") as f:
        s3.upload_fileobj(f, aws_bucket, object_name)
    
    os.remove(csv_filename)
    logging(f"Completed extracting data from postgres database loaded to {object_name}")


def extract_api_aws():
    # authenticate and download data from Kaggle API
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files('carrie1/ecommerce-data', path='./data/')

    # Extract the ZIP file to the temporary directory
    temp_dir = tempfile.mkdtemp()  # Create a temporary directory
    zip_path = './data/ecommerce-data.zip' # destination path
    shutil.unpack_archive(zip_path, temp_dir, format='zip')
    extracted_file_path = os.path.join(temp_dir, 'data.csv')

    # authenticate upload to S3
    key = pd.read_csv(aws_credentials_path)
    _aws_access_key_id = key["Access key ID"][0]
    _aws_secret_access_key = key["Secret access key"][0]

    s3 = boto3.client('s3',
                        aws_access_key_id=_aws_access_key_id,
                        aws_secret_access_key=_aws_secret_access_key)

    object_name = 'data_api_uncleaned.csv'
    s3.upload_file(extracted_file_path, aws_bucket, object_name)
    os.remove(zip_path)
    logging.info(f"Completed extracting data from API loaded to {object_name}")


# def clean_aws():
#     # Instantiate a GCS client with gcsfs to be able to handle the file with pandas
#     fs = gcsfs.GCSFileSystem(project=project_id, token=credentials_path)

#     # Read the data file using pandas
#     with fs.open(f"{bucket_name}/{source_file}") as file:
#         df = pd.read_csv(file, encoding='cp1252')

#     # Clean the data
#     df['Description'] = df['Description'].fillna('No Description')
#     df['CustomerID'] = df['CustomerID'].fillna(0)
#     df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
#     df['total_spend'] = df['Quantity'] * df['UnitPrice']

#     # Convert the data types
#     df['InvoiceNo'] = df['InvoiceNo'].astype(str)
#     df['StockCode'] = df['StockCode'].astype(str)
#     df['Description'] = df['Description'].astype(str)
#     df['CustomerID'] = df['CustomerID'].astype(float).astype(int)
#     df['Country'] = df['Country'].astype(str)
#     df['total_spend'] = df['total_spend'].astype(float)

#     # Replace several descriptions with the most frequent description for each stock code
#     df['StockCode'] = df['StockCode'].str.upper()
#     most_freq = df.groupby('StockCode')['Description'].agg(lambda x: x.value_counts().idxmax()).reset_index()
#     columns_index = df.columns
#     df = df.drop(columns=['Description'])
#     df = pd.merge(df, most_freq, on='StockCode', how='left')
#     df = df.reindex(columns=columns_index)

#     # Write the cleaned data to a new parquet file
#     df.to_parquet(destination_file, index=False)

#     # Upload the cleaned data to GCS
#     fs.put(destination_file, f"{bucket_name}/staging_area/{destination_file}")

#     # Optionally, delete the local downloaded data file
#     os.remove(destination_file)