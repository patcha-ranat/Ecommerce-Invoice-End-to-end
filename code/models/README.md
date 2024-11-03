# Interpretable Dynamic Customer Segmentation
*with RFM, KMeans, and LightGBM*

*patcharanat p.*

## Usage
- please, refer to [input_example.sh](./test/input_example.sh)
```bash
# Example for local execution
python main.py --env local --method filesystem --input_path '../../data/ecomm_invoice_transaction.parquet' --output_path output --exec_date 2024-10-29
```

## Usage via Docker Container
```bash
# workdir code/models

# docker build -t <image_name:tag> .
docker build -t ecomm/interpretable-dynamic-rfm-service:v1 .

# docker run --name <container_name> <image_name:tag> <app_args1> ...

# --rm: to remove container after runtime

# copy data/ecomm_invoice_transaction.parquet to code/models

# with python main.py as an entrypoint
docker run --rm \
    ecomm/interpretable-dynamic-rfm-service:v1 \
    --env local --method filesystem \
    --input_path 'ecomm_invoice_transaction.parquet' \
    --output_path output --exec_date 2024-10-29

# with bash as en entrypoint (cmd)
docker run --rm -it ecomm/interpretable-dynamic-rfm-service:v1
python main.py ...
```

## Cloud Authentication
```bash
gcloud auth application-default login

gcloud config set project <project_id>

# for impersonated account
# gcloud auth application-default login --impersonate-service-account <SERVICE_ACCT_EMAIL>
```

## Terraform
- Prerequisite: `gcloud`
- please refer to this [official documentation](https://cloud.google.com/docs/terraform/authentication) for official authentication approaches.
```bash
cd terraform

terraform init
terraform plan
terraform apply
terraform destroy
```

## Usgae via CLoud (GCP)
```bash
# apply terraform to create bucket or bigquery dataset
# workdir: code/models/terraform


# workdir: code/models
# ./test/initial_gcp.sh
cp ../../data/ecomm_invoice_transaction.parquet .

gsutil cp ecomm_invoice_transaction.parquet gs://<landing/uri>

rm ecomm_invoice_transaction.parquet


# ./test/input_sample.sh
python main.py --env gcp --project_id <project_id> --method filesystem --input_path 'gs://<landing_bucket_name>/input/data/2024-11-03/ecomm_invoice_transaction.parquet' --output_path 'gs://<staging_bucket_name>/output' --exec_date 2024-11-03
```
