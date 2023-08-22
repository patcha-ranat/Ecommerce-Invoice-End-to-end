import pandas as pd
from google.cloud import storage
import os
import gcsfs
from google.cloud import bigquery


def clean_data_google(source_file, destination_file, project_id, bucket_name, credentials_path):
    
    # Instantiate a GCS client with gcsfs to be able to handle the file with pandas
    fs = gcsfs.GCSFileSystem(project=project_id, token=credentials_path)

    # Read the data file using pandas
    with fs.open(f"{bucket_name}/{source_file}") as file:
        df = pd.read_csv(file, encoding='cp1252')

    # Clean the data
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

    # Write the cleaned data to a new parquet file
    df.to_parquet(destination_file, index=False)

    # Upload the cleaned data to GCS
    fs.put(destination_file, f"{bucket_name}/staging_area/{destination_file}")

    # Optionally, delete the local downloaded data file
    os.remove(destination_file)


def load_data_google(project_id, bucket_name, dataset_name, table_name, credentials_path):

    # Construct a BigQuery client object.
    client = bigquery.Client.from_service_account_json(credentials_path)

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = f"{project_id}.{dataset_name}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        # uncomment to overwrite the table.
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.PARQUET,
    )
    uri = f"gs://{bucket_name}/staging_area/ecomm_invoice_transaction.parquet"

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))


def load_data_google_autodetect(project_id, bucket_name, dataset_name, table_name, credentials_path):
    # Construct a BigQuery client object.
    client = bigquery.Client.from_service_account_json(credentials_path)

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = f"{project_id}.{dataset_name}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        autodetect=True, source_format="PARQUET" # autodetect is omittable if source_format are parquet, avro or orc
    )
    uri = f"gs://{bucket_name}/staging_area/ecomm_invoice_transaction.parquet"
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.
    load_job.result()  # Waits for the job to complete.
    destination_table = client.get_table(table_id)
    print("Loaded {} rows to an auto-detected table.".format(destination_table.num_rows))


def clear_staging_area(bucket_name, blob_name, credentials_path):
    """Deletes a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"

    storage_client = storage.Client.from_service_account_json(credentials_path)

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.get_blob(blob_name)
    generation_match_precondition = None

    # Optional: set a generation-match precondition to avoid potential race conditions
    # and data corruptions. The request to delete is aborted if the object's
    # generation number does not match your precondition.
    blob.reload()  # Fetch blob metadata to use in generation_match_precondition.
    generation_match_precondition = blob.generation

    blob.delete(if_generation_match=generation_match_precondition)

    print(f"Blob {blob_name} deleted.")

if __name__ == "__main__":
    pass