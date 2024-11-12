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

variable "region" {
    description = "Region for GCP resources"
    type = string
    default = "asia-southeast1"
}

# Workload Identity Federation

variable "pool_id" {
    type = string
    default = "kde-wif-pool-dev"
}

variable "pool_display_name" {
    type = string
    default = "kde-wif-pool-dev"
}

variable "pool_description" {
    type = string
    default = "kde ecomm auth pool"
}

variable "provider_id" {
    type = string
    default = "kde-wif-provider-dev"
}

variable "provider_display_name" {
    type = string
    default = "kde-wif-provider-dev"
}

variable "provider_description" {
    type = string
    default = "kde ecomm auth provider"
}

variable "attribute_mapping" {
    type = map(any)
    default = {
        "google.subject"                = "assertion.sub"
        "attribute.aud"                 = "assertion.aud"
        "attribute.repository_owner"    = "assertion.repository_owner"
    }
}

variable "attribute_condition" {
    type = string
    default = <<EOT
        assertion.repository_owner == "patcha-ranat"
    EOT
}

variable "issuer_uri" {
    type = string
    default = "https://token.actions.githubusercontent.com"
}