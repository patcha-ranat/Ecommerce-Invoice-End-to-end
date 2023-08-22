terraform {
  required_version = "1.5.3"
  backend "local" {}  # Can change from "local" to "gcs" (for google) or "s3" (for aws), if you would like to preserve your tf-state online
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.61.0"
    }
    aws = {
      source  = "hashicorp/aws"
      # version = "~> 4.0"
    }
  }
}

# google cloud platform: GCP
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
        "name": "InvoiceNo",
        "type": "STRING"
      },
      {
        "name": "StockCode",
        "type": "STRING"
      },
      {
        "name": "Description",
        "type": "STRING"
      },
      {
        "name": "Quantity",
        "type": "INTEGER"
      },
      {
        "name": "InvoiceDate",
        "type": "TIMESTAMP"
      },
      {
        "name": "UnitPrice",
        "type": "FLOAT"
      },
      {
        "name": "CustomerID",
        "type": "INTEGER"
      },
      {
        "name": "Country",
        "type": "STRING"
      },
      {
        "name": "total_spend",
        "type": "FLOAT"
      }
    ]
EOF
}

# # Not usable if data is not already exist in GCS
# resource "google_bigquery_table" "external" {
#   dataset_id = google_bigquery_dataset.dataset.dataset_id
#   table_id   = var.EXTERNAL_TABLE_ID
#   deletion_protection = false
#   external_data_configuration {
#     autodetect    = true # only use for csv, json, and google sheets data
#     source_format = "PARQUET"
#     source_uris = [
#       "gs://${local.data_lake_bucket}/staging_area/ecomm_invoice_transaction.parquet",
#     ]
#   }
# }

# Artifact Registry for deployment
resource "google_artifact_registry_repository" "ecomm-invoice-repo" {
  location = var.region
  repository_id = "ecomm-invoice-registry"
  description = "default description"
  format = "python"
  # kms_key_name = "KEY" # omit this to use a default setting
}

# Amazon Web Services: AWS

provider "aws" {
  region = "ap-southeast-1"
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key
}

resource "aws_s3_bucket" "aws_bucket" {
  bucket = "ecomm-invoice-bucket-aws"
  
  force_destroy = true
  # versioning {
  #   enabled = true
  # }
}

# # Redshift serverless
# # Create the Redshift Serverless Network
# # AWS Availability Zones data
# data "aws_availability_zones" "available" {}

# ######################################

# # Create the VPC
# resource "aws_vpc" "redshift-serverless-vpc" {
#   cidr_block           = var.redshift_serverless_vpc_cidr
#   enable_dns_hostnames = true
  
#   tags = {
#     Name        = "ecomm-invoice-kde-redshift-serverless-vpc"
#     Environment = var.app_environment
#   }
# }

# ######################################

# # Create the Redshift Subnet AZ1
# resource "aws_subnet" "redshift-serverless-subnet-az1" {
#   vpc_id            = aws_vpc.redshift-serverless-vpc.id
#   cidr_block        = var.redshift_serverless_subnet_1_cidr
#   availability_zone = data.aws_availability_zones.available.names[0]
  
#   tags = {
#     Name        = "ecomm-invoice-kde-redshift-serverless-subnet-az1"
#     Environment = var.app_environment
#   }
# }

# # Create the Redshift Subnet AZ2
# resource "aws_subnet" "redshift-serverless-subnet-az2" {
#   vpc_id            = aws_vpc.redshift-serverless-vpc.id
#   cidr_block        = var.redshift_serverless_subnet_2_cidr
#   availability_zone = data.aws_availability_zones.available.names[1]
  
#   tags = {
#     Name        = "ecomm-invoice-kde-redshift-serverless-subnet-az2"
#     Environment = var.app_environment
#   }
# }

# # Create the Redshift Subnet AZ3
# resource "aws_subnet" "redshift-serverless-subnet-az3" {
#   vpc_id            = aws_vpc.redshift-serverless-vpc.id
#   cidr_block        = var.redshift_serverless_subnet_3_cidr
#   availability_zone = data.aws_availability_zones.available.names[2]
  
#   tags = {
#     Name        = "ecomm-invoice-kde-redshift-serverless-subnet-az3"
#     Environment = var.app_environment
#   }
# }

# resource "aws_security_group" "redshift-serverless-security-group" {
#   depends_on = [aws_vpc.redshift-serverless-vpc]

#   name        = "ecomm-invoice-kde-redshift-serverless-security-group"
#   description = "ecomm-invoice-kde-redshift-serverless-security-group"

#   vpc_id = aws_vpc.redshift-serverless-vpc.id
  
#   ingress {
#     description = "Redshift port"
#     from_port   = 5439
#     to_port     = 5439
#     protocol    = "tcp"
#     cidr_blocks = ["10.0.0.0/24"] // update this to secure the connection to Redshift
#   }
  
#   tags = {
#     Name        = "ecomm-invoice-kde-redshift-serverless-security-group"
#     Environment = var.app_environment
#   }
# }

# # Create the Redshift Serverless IAM Role
# resource "aws_iam_role" "redshift-serverless-role" {
#   name = "ecomm-invoice-kde-redshift-serverless-role"

# assume_role_policy = <<EOF
# {
#     "Version": "2012-10-17",
#     "Statement": [
#         {
#             "Effect": "Allow",
#             "Principal": {
#                 "Service": [
#                     "redshift-serverless.amazonaws.com",
#                     "redshift.amazonaws.com",
#                     "sagemaker.amazonaws.com"
#                 ]
#             },
#             "Action": "sts:AssumeRole"
#         }
#     ]
# }
# EOF

#   tags = {
#     Name        = "ecomm_invoice-kde-redshift-serverless-role"
#     Environment = var.app_environment
#   }
# }

# # Create and assign an IAM Role Policy to access S3 Buckets
# resource "aws_iam_role_policy" "redshift-s3-full-access-policy" {
#   name = "AmazonRedshift-CommandsAccessPolicy-ecomm-invoice-kde"
#   role = aws_iam_role.redshift-serverless-role.id

# policy = <<EOF
# {
#    "Version": "2012-10-17",
#    "Statement": [
#      {
#        "Effect": "Allow",
#        "Action": [
#             "s3:*",
#             "iam:ListPolicies"
#         ],
#        "Resource": "*"
#       }
#    ]
# }
# EOF
# }

# # Get the AmazonRedshiftAllCommandsFullAccess policy
# data "aws_iam_policy" "redshift-full-access-policy" {
#   name = "AmazonRedshiftAllCommandsFullAccess"
# }

# # Attach the policy to the Redshift role
# resource "aws_iam_role_policy_attachment" "attach-s3" {
#   role       = aws_iam_role.redshift-serverless-role.name
#   policy_arn = data.aws_iam_policy.redshift-full-access-policy.arn
# }

# # Create the Redshift Serverless Namespace
# resource "aws_redshiftserverless_namespace" "serverless" {
#   namespace_name      = var.redshift_serverless_namespace_name
#   db_name             = var.redshift_serverless_database_name
#   admin_username      = var.redshift_serverless_admin_username
#   admin_user_password = var.redshift_serverless_admin_password
#   iam_roles           = [aws_iam_role.redshift-serverless-role.arn]

#   tags = {
#     Name        = var.redshift_serverless_namespace_name
#     Environment = var.app_environment
#   }
# }

# # Create the Redshift Serverless Workgroup
# resource "aws_redshiftserverless_workgroup" "serverless" {
#   depends_on = [aws_redshiftserverless_namespace.serverless]

#   namespace_name = aws_redshiftserverless_namespace.serverless.id
#   workgroup_name = var.redshift_serverless_workgroup_name
#   base_capacity  = var.redshift_serverless_base_capacity
  
#   security_group_ids = [ aws_security_group.redshift-serverless-security-group.id ]
#   subnet_ids         = [ 
#     aws_subnet.redshift-serverless-subnet-az1.id,
#     aws_subnet.redshift-serverless-subnet-az2.id,
#     aws_subnet.redshift-serverless-subnet-az3.id,
#   ]
#   publicly_accessible = var.redshift_serverless_publicly_accessible
  
#   tags = {
#     Name        = var.redshift_serverless_workgroup_name
#     Environment = var.app_environment
#   }
# }

# redshift traditional cluster
# resource "aws_redshift_cluster" "ecomm_invoice_cluster" {
#   cluster_identifier         = "ecomm-invoice-cluster"
#   node_type                  = "dc1.large"
#   cluster_type               = "single-node"
#   # number_of_nodes            = 2
#   cluster_subnet_group_name  = "ecomm-invoice-subnet-group"  # Create this separately or use an existing one.
#   publicly_accessible        = false

#   # Database settings
#   database_name              = "mydb"
#   master_username            = "admin"
#   master_password            = "Admin123"

#   # Enable automatic backups and set retention period (in days)
#   # automated_snapshot_retention_period = 7

#   # Enable encryption at rest
#   # Note: You need to create a KMS key for this or use an existing one.
#   # encryption_type             = "KMS"
#   # kms_key_id                  = "your-kms-key-id"

#   # Enable enhanced VPC routing for better performance in VPC
#   # enhanced_vpc_routing       = true

#   # Allow version upgrade if a new version of Redshift becomes available
#   allow_version_upgrade      = true

#   cluster_parameter_group_name = "ecomm-invoice-groupname"

#   # Enable force destroy to remove the Redshift cluster and all its resources forcefully.
#   force_destroy = true

#   # Tags for the Redshift cluster
#   tags = {
#     Environment = "Development"
#     Project     = "DataWarehouse"
#   }
# }