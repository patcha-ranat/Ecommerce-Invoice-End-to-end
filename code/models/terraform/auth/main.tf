terraform {
    required_providers {
        google = {
            source = "hashicorp/google"
            version = "6.9.0"
        }
    }
}

provider "google" {
    impersonate_service_account = var.service_account_email
    project     = var.project_id
    region      = var.region
}

# Workload Identity Federation

resource "google_iam_workload_identity_pool" "main_pool" {
    project                   = var.project_id
    workload_identity_pool_id = var.pool_id
    display_name              = var.pool_display_name
    description               = var.pool_description
    disabled                  = false
}

resource "google_iam_workload_identity_pool_provider" "main_provider" {
    project                            = var.project_id
    workload_identity_pool_id          = google_iam_workload_identity_pool.main_pool.workload_identity_pool_id
    workload_identity_pool_provider_id = var.provider_id
    display_name                       = var.provider_display_name
    description                        = var.provider_description
    attribute_mapping                  = var.attribute_mapping
    attribute_condition                = var.attribute_condition
    oidc {
        issuer_uri        = var.issuer_uri
    }
}