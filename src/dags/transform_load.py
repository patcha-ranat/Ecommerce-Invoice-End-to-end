import pandas as pd
from google.cloud import storage
import os
import gcsfs


def clean_data(source_file, destination_file, project_id, bucket_name, credentials_path):
    # Instantiate a storage client
    # storage_client = storage.Client.from_service_account_json(credentials_path)
    
    # Instantiate a GCS client with gcsfs
    fs = gcsfs.GCSFileSystem(project=project_id, token=credentials_path)
    
    # Specify csv path name
    # csv_file_path = f"gs://{bucket_name}/{source_file}"

    # Read the data file using pandas
    with fs.open(f"{bucket_name}/{source_file}") as file:
        # df = pd.read_csv(csv_file_path, encoding='cp1252')
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

    # standardize columns name
    df.columns = df.columns.str.lower()

    # Write the cleaned data to a new parquet file
    df.to_parquet(destination_file)

    # Upload the cleaned data to GCS
    fs.put(destination_file, f"{bucket_name}/staging_area/{destination_file}")
    # bucket = storage_client.bucket(bucket_name)
    # blob = bucket.blob(destination_file)
    # blob.upload_from_filename(f"./staging_area/{destination_file}")

    # Optionally, delete the local downloaded data file
    os.remove(destination_file)

def load_data_external():
    pass

def load_data():
    pass

if __name__ == "__main__":
    pass