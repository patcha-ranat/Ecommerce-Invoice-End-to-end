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

variable "EXTERNAL_TABLE_ID" {
  description = "your external table name in BigQuery"
  type = string
  default = "external_ecomm_invoice_transaction"
}

# variable "BUCKET_NAME" {
#     description = "Unique name for your bucket"
#     defualt = "ecomm-invoice-data-lake-bucket"
# } -> use local instead

# aws IAM
# retrieve from `terraform.tfvars`
variable "aws_access_key" {
  type = string
  description = "AWS access key"
}
variable "aws_secret_key" {
  type = string
  description = "AWS secret key"
}

# serverless variables
variable "redshift_serverless_namespace_name" {
  type        = string
  description = "Redshift Serverless Namespace Name"
  default = "ecomm-invoice-namespace"
}

variable "redshift_serverless_database_name" { 
  type        = string
  description = "Redshift Serverless Database Name"
  default = "mydb"
}

variable "redshift_serverless_admin_username" {
  type        = string
  description = "Redshift Serverless Admin Username"
  default = "admin"
}

variable "redshift_serverless_admin_password" { 
  type        = string
  description = "Redshift Serverless Admin Password"
  default = "Admin123"
}

variable "redshift_serverless_workgroup_name" {
  type        = string
  description = "Redshift Serverless Workgroup Name"
  default = "ecomm-invoice-workgroup"
}

variable "redshift_serverless_base_capacity" {
  type        = number
  description = "Redshift Serverless Base Capacity"
  default     = 32 // 32 RPUs to 512 RPUs in units of 8 (32,40,48...512)
}

variable "redshift_serverless_publicly_accessible" {
  type        = bool
  description = "Set the Redshift Serverless to be Publicly Accessible"
  default     = false
}

# network for redshift serverless
variable "redshift_serverless_vpc_cidr" {
  type        = string
  description = "VPC IPv4 CIDR"
}

variable "redshift_serverless_subnet_1_cidr" {
  type        = string
  description = "IPv4 CIDR for Redshift subnet 1"
}

variable "redshift_serverless_subnet_2_cidr" {
  type        = string
  description = "IPv4 CIDR for Redshift subnet 2"
}

variable "redshift_serverless_subnet_3_cidr" {
  type        = string
  description = "IPv4 CIDR for Redshift subnet 3"
}

variable "app_environment" {
  type=string
  description="Environment for the application"
}