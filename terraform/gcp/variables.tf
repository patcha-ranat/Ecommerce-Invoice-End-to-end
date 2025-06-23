variable "service_account_email" {
  description = "Used for Account impersonation with ADC"
  type        = string
  sensitive   = true
}

variable "project_id" {
  description = "Project ID"
  type        = string
  sensitive   = true
}

variable "env" {
  type    = string
  default = "dev"
}

variable "region" {
  description = "Region for GCP resources"
  type        = string
  default     = "asia-southeast1"
}

# Google Cloud Storage

variable "bucket_region" {
  type    = string
  default = "ASIA-SOUTHEAST1"
}

variable "bucket_landing_name" {
  type    = string
  default = "kde_ecomm_landing"
}

variable "bucket_staging_name" {
  type    = string
  default = "kde_ecomm_staging"
}

variable "bucket_curated_name" {
  type    = string
  default = "kde_ecomm_curated"
}

variable "bucket_artifact_name" {
  type    = string
  default = "kde_ecomm_artifact"
}

# BigQuery

variable "dataset_id_silver" {
  type    = string
  default = "kde_ecomm_silver"
}

variable "dataset_id_gold" {
  type    = string
  default = "kde_ecomm_gold"
}
