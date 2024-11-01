variable "service_account_email" {
    description = "Used for Account impersonation with ADC"
    type = string
    sensitive = true
}

variable "project_id" {
    description = "Project ID"
    type = string
    sensitive = true
}

variable "env" {
    default = "dev"
    type = string
}

variable "region" {
    description = "Region for GCP resources"
    default = "asia-southeast1"
    type = string
}

variable "bucket_region" {
    default = "ASIA-SOUTHEAST1"
    type = string
}

variable "bucket_landing_name" {
    default = "kde_ecomm_landing"
    type = string
}

variable "bucket_staging_name" {
    default = "kde_ecomm_staging"
    type = string
}

variable "bucket_curated_name" {
    default = "kde_ecomm_curated"
    type = string
}

variable "dataset_id_silver" {
    default = "kde_ecomm_silver"
    type = string
}

variable "dataset_id_gold" {
    default = "kde_ecomm_gold"
    type = string
}