locals {
  data_lake_bucket = "ecomm-invoice-data-lake-bucket"
}

# google resources
variable "project" {
  description = "Your Project ID"
  default = "fabled-rookery-386802"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "asia-southeast1"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that cleaned data (from GCS) will be written to"
  type = string
  default = "ecomm_invoice"
}

variable "TABLE_ID" {
  description = "your table name in BigQuery"
  type = string
  default = "ecomm_invoice_transaction"
}

# variable "BUCKET_NAME" {
#     description = "Unique name for your bucket"
#     defualt = "ecomm-invoice-data-lake-bucket"
# } -> use local instead

# aws IAM
# retrieve from `secrets.tfvars`
variable "aws_access_key" {
  type = string
  description = "AWS access key"
}
variable "aws_secret_key" {
  type = string
  description = "AWS secret key"
}