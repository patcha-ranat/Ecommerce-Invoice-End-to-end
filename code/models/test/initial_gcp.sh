# upload input data to gcs manually

# gcloud authenticate for gsutil
# gcloud auth application-default login
# gcloud config set project <project_id>

cp ../../data/ecomm_invoice_transaction.parquet .

gsutil cp ecomm_invoice_transaction.parquet gs://<landing_bucket_name>/input/data/2024-11-03/

rm ecomm_invoice_transaction.parquet
