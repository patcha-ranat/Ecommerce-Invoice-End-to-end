terraform {
    required_providers {
        google = {
            source = "hashicorp/google"
            version = "6.9.0"
        }
    }
}

provider "google" {
    # impersonate_service_account = var.service_account_email # use ADC for local development
    project     = var.project_id
    region      = var.region
}

# Cloud Storage

resource "google_storage_bucket" "bucket_dev" {
    name          = var.bucket_dev_name
    location      = var.bucket_region
    force_destroy = true
}

resource "google_storage_bucket" "bucket_stg" {
    name          = var.bucket_stg_name
    location      = var.bucket_region
    force_destroy = true
}

resource "google_storage_bucket" "bucket_curated" {
    name          = var.bucket_curated_name
    location      = var.bucket_region
    force_destroy = true
}

# BigQuery

resource "google_bigquery_dataset" "dataset_silver" {
    dataset_id                  = var.dataset_id_silver
    location                    = var.region
    delete_contents_on_destroy = true

    labels = {
        env = var.env
    }
}

resource "google_bigquery_dataset" "dataset_gold" {
    dataset_id                  = var.dataset_id_gold
    location                    = var.region
    delete_contents_on_destroy = true

    labels = {
        env = var.env
    }
}