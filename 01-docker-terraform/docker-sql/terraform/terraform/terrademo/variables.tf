###############################################################
# variables.tf
# This file defines input variables for the Terraform project.
# These variables are referenced in main.tf to configure resources
# such as credentials, project settings, region, BigQuery datasets,
# and Google Cloud Storage buckets. Adjust these values as needed
# for your deployment.
###############################################################

# Path to the Google Cloud service account credentials JSON file.
variable "credentials" {
  description = "My Credentials"
  default     = "./keys/mycreds.json"
}

# The Google Cloud project ID where resources will be created.
variable "project" {
  description = "Project"
  default     = "terraform-demo-488101"
}

# The Google Cloud region for resource deployment.
variable "region" {
  description = "Region"
  default     = "europe-southwest1"
}

# The location/region for BigQuery and GCS resources (e.g., EU, US).
variable "location" {
  description = "Project Location"
  default     = "EU"
}

# Name of the BigQuery dataset to be created or managed.
variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "demo_dataset"
}

# Name of the Google Cloud Storage bucket to be created or managed.
variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "terraform-demo-488101-terra-bucket"
}

# Storage class for the GCS bucket (e.g., STANDARD, NEARLINE).
variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}