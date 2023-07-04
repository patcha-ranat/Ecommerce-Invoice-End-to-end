terraform {
  required_version = ">= 1.0"
  backend "local" {}  # Can change from "local" to "gcs" (for google) or "s3" (for aws), if you would like to preserve your tf-state online
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}

provider "google" {
  credentials = file("../credentials/gcs_credentials.json")
  project     = var.project
  region      = var.region
}

# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "gcs_bucket" {
  name          = local.data_lake_bucket
  location      = var.region
  
  # Optional, but recommended settings:
  storage_class = var.storage_class
  uniform_bucket_level_access = true
  force_destroy = true
  public_access_prevention = "enforced"

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 7  // days
    }
  }
}

# skip this if you manually create service account via GCP UI (and granted permission)
# resource "google_storage_bucket_iam_binding" "bucket_iam_binding" {
#   bucket = google_storage_bucket.gcs_bucket.name
#   role   = "roles/storage.admin"

#   members = [
#     "user:<USER_EMAIL>",
#     "group:<GROUP_EMAIL>",
#     "serviceAccount:<SERVICE_ACCOUNT_EMAIL>",
#   ]
# }

# resource "google_storage_bucket_iam_member" "bucket_iam_member" {
#   bucket = google_storage_bucket.bucket.name
#   role   = "roles/storage.objectViewer"
#   member = "allUsers"
# }

# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.BQ_DATASET
  project    = var.project
  location   = var.region
}

resource "google_bigquery_table" "table" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  project    = var.project
  table_id   = var.TABLE_ID
  deletion_protection  = false
  
  schema = <<EOF
    [
      {
        "name": "invoiceno",
        "type": "STRING"
      },
      {
        "name": "stockcode",
        "type": "STRING"
      },
      {
        "name": "description",
        "type": "STRING"
      },
      {
        "name": "quantity",
        "type": "INTEGER"
      },
      {
        "name": "invoicedate",
        "type": "TIMESTAMP"
      },
      {
        "name": "unitprice",
        "type": "FLOAT"
      },
      {
        "name": "customerid",
        "type": "INTEGER"
      },
      {
        "name": "country",
        "type": "STRING"
      },
      {
        "name": "total_spend",
        "type": "FLOAT"
      }
    ]
EOF
}