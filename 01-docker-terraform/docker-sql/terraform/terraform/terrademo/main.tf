###############################################################
# main.tf
# This is the main Terraform configuration file for provisioning
# Google Cloud resources. It defines the provider, configures
# authentication, and manages resources such as a Google Cloud
# Storage bucket and a BigQuery dataset. All variables are defined
# in variables.tf for easy customization.
###############################################################

# Specify the required provider and its version for Google Cloud.
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.20.0"
    }
  }
}

# Provider configuration:
# - Uses credentials from the specified JSON file
# - Sets the project and region for resource creation
# Credentials can be set in variables.tf or exported via:
#   export GOOGLE_CREDENTIALS="path/to/your/credentials.json"
# Use 'echo $GOOGLE_CREDENTIALS' to verify the credentials path before running terraform commands.
provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}


# Next step will be to create a bucket
# Variable in resource has to be globally unique; this means that the bucket name has to be unique across all Google Cloud Storage buckets
# Name of the bucket can be project specific, for example, project name + bucket name or terraform will generate a random name

# Run 'terraform init' to initialize the working directory and download provider plugins.
# Resource: Google Cloud Storage Bucket
# - Creates a globally unique bucket for the project
# - Bucket name is set via the gcs_bucket_name variable
# - Location and force_destroy are set for flexibility and cleanup
# - Includes a lifecycle rule to abort incomplete multipart uploads after 1 day
resource "google_storage_bucket" "demo-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

# To preview changes, run: 'terraform plan'
# To apply changes, run: 'terraform apply'

# Resource: BigQuery Dataset
# - Creates a BigQuery dataset with the specified name and location
# - Dataset name and location are set via variables
resource "google_bigquery_dataset" "demo_dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}

# Formatting and Usage:
# - Run 'terraform fmt' to format this file
# - Use 'terraform plan' to see the planned changes
# - Use 'terraform apply' to provision resources