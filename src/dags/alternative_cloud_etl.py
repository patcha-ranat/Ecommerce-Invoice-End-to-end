from kaggle.api.kaggle_api_extended import KaggleApi
import os
import shutil
import tempfile
import boto3
import pandas as pd
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
import awswrangler as wr
import logging
import psycopg2
import zipfile

aws_credentials_path = os.environ["AWS_CREDENTIALS_PATH"]
aws_bucket = os.environ["AWS_BUCKET"]
_redshift_host = ''
_redshift_role_arn = ''

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
        postgres_conn_id="postgres-source",
        schema="mydatabase"
    )
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Define the SQL query to extract data
    query = "SELECT * FROM myschema.ecomm_invoice"

    # specify filename
    csv_file = "/opt/airflow/data/unloaded_data.csv"

    # Define the COPY command with the query, CSV format, and headers
    copy_command = f"COPY ({query}) TO STDOUT WITH CSV HEADER"

    with open(csv_file, "w", encoding='utf-8') as f: # use "w+" to create file if it not exist
        cursor.copy_expert(copy_command, file=f)
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
    
    with open(csv_file, "rb") as f:
        # s3.upload_fileobj(f, aws_bucket, object_name)
        s3.put_object(Bucket=aws_bucket, Key=object_name, Body=f)
    f.close()
    
    os.remove(csv_file)
    logging.info(f"Completed extracting data from postgres database loaded to {object_name}")


def extract_api_aws():
    # authenticate and download data from Kaggle API
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files('carrie1/ecommerce-data', path='./data/')

    path_to_zip_file = './data/ecommerce-data.zip'
    with zipfile.ZipFile(path_to_zip_file, "r") as zip_ref:
        zip_ref.extractall('./data/')

    extracted_file_path = os.path.join('./data/', 'data.csv')

    # authenticate upload to S3
    key = pd.read_csv(aws_credentials_path)
    _aws_access_key_id = key["Access key ID"][0]
    _aws_secret_access_key = key["Secret access key"][0]

    s3 = boto3.client('s3',
                        aws_access_key_id=_aws_access_key_id,
                        aws_secret_access_key=_aws_secret_access_key)

    object_name = 'data_api_uncleaned.csv'
    s3.upload_file(extracted_file_path, aws_bucket, object_name)
    # in case not remove before google task load to GCS
    # os.remove(path_to_zip_file)
    logging.info(f"Completed extracting data from API loaded to {object_name}")


def clean_aws(bucket_name, object_name, destination_file):
    # retrieve credentials
    key = pd.read_csv(aws_credentials_path)
    _aws_access_key_id = key["Access key ID"][0]
    _aws_secret_access_key = key["Secret access key"][0]

    # Authenticate by session
    session = boto3.Session(
        aws_access_key_id = _aws_access_key_id,
        aws_secret_access_key = _aws_secret_access_key
    )
    
    df = wr.s3.read_csv(path=f"s3://{bucket_name}/{object_name}",
                        boto3_session=session,
                        encoding='cp1252')

    # Clean the data with old script
    df['Description'] = df['Description'].fillna('No Description')
    df['CustomerID'] = df['CustomerID'].fillna(0)
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    df['total_spend'] = df['Quantity'] * df['UnitPrice']

    # Convert the data types
    df['InvoiceNo'] = df['InvoiceNo'].astype(str)
    df['StockCode'] = df['StockCode'].astype(str)
    df['Description'] = df['Description'].astype(str)
    df['CustomerID'] = df['CustomerID'].astype(float).astype(int)
    df['Country'] = df['Country'].astype(str)
    df['total_spend'] = df['total_spend'].astype(float)

    # Replace several descriptions with the most frequent description for each stock code
    df['StockCode'] = df['StockCode'].str.upper()
    most_freq = df.groupby('StockCode')['Description'].agg(lambda x: x.value_counts().idxmax()).reset_index()
    columns_index = df.columns
    df = df.drop(columns=['Description'])
    df = pd.merge(df, most_freq, on='StockCode', how='left')
    df = df.reindex(columns=columns_index)

    # Upload the cleaned data to S3 Staging Area
    df.to_parquet(destination_file, index=False)
    s3 = boto3.client('s3',
                      aws_access_key_id=_aws_access_key_id,
                      aws_secret_access_key=_aws_secret_access_key)
    s3.upload_file(destination_file, bucket_name, f"staging_area/{destination_file}")

def load_data_aws():
    # # retrieve credentials
    # key = pd.read_csv(aws_credentials_path)
    # _aws_access_key_id = key["Access key ID"][0]
    # _aws_secret_access_key = key["Secret access key"][0]

    # # authenticate and upload to S3
    # s3 = boto3.client('s3',
    #                     aws_access_key_id=_aws_access_key_id,
    #                     aws_secret_access_key=_aws_secret_access_key)
    
    object_key = "staging_area/ecomm_invoice_transaction.parquet"

    # Configure your Redshift connection details
    redshift_host = _redshift_host # need to change everytime new created
    redshift_dbname = 'mydb'
    redshift_user = 'admin'
    redshift_password = 'Admin123'
    redshift_port = '5439'

    # Establish a connection to Redshift
    conn = psycopg2.connect(
        host        = redshift_host,
        dbname      = redshift_dbname,
        user        = redshift_user,
        password    = redshift_password,
        port        = redshift_port
    )
    cur = conn.cursor()

    # # Use boto3 to read a sample of the file from S3 into a DataFrame for creating table dynamically
    # s3 = boto3.client('s3', aws_access_key_id=_aws_access_key_id, aws_secret_access_key=_aws_secret_access_key)
    # file_obj = s3.get_object(Bucket=aws_bucket, Key=object_key)
    # df_sample = pd.read_parquet(file_obj['Body'], nrows=10)  # Read the first 10 rows as a sample

    # # Infer the column names and data types from the DataFrame sample
    # column_names = df_sample.columns.tolist()
    # column_data_types = {col: str(df_sample.dtypes[col]) for col in column_names}

    # # Generate the CREATE TABLE SQL statement dynamically
    # create_table_sql = f"CREATE TABLE IF NOT EXISTS my_dynamic_table ("
    # for col_name, data_type in column_data_types.items():
    #     create_table_sql += f"{col_name} {data_type}, "
    # create_table_sql = create_table_sql.rstrip(', ') + ");"

    # Create the target table in Redshift (if it doesn't exist)
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS ecomm_invoice_transaction (
        InvoiceNo     STRING,
        StockCode     STRING,
        Description   STRING,
        Quantity      INTEGER,
        InvoiceDate   TIMESTAMP,
        UnitPrice     FLOAT,
        CustomerID    INTEGER,
        Country       STRING,
        total_spend   FLOAT
    );
    """
    cur.execute(create_table_sql)
    conn.commit()

    # Load the data from the DataFrame into the Redshift table
    copy_command = f"""
                    COPY ecomm_invoice_transaction FROM 's3://{aws_bucket}/{object_key}'
                    IAM_ROLE '{_redshift_role_arn}'
                    FORMAT AS PARQUET;
                    """
    
    cur.execute(copy_command)
    conn.commit()

    # Close the connection
    conn.close()

if __name__ == "__main__":
    pass