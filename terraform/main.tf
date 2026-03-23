# Terraform configuration for Google Cloud Platform
terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.51.0"
    }
  }
}

# Provider block: Configures the connection to your GCP project
provider "google" {
  # RUN 'gcloud config get-value project' in your terminal to find your ID
  project = "project-1314e5d0-dc4a-4a2f-804"
  region  = "us-central1"
  # Create a credentials.json file in the terraform directory and export the path to the credentials file, then make it permanent in the .bashrc file
  # export GOOGLE_APPLICATION_CREDENTIALS=$(pwd)/credentials.json

}

# 1. Data Lake Bucket: Storage for raw CSV files
resource "google_storage_bucket" "rioja_data_lake" {
  name          = "rioja_wine_lake_raw" # Must be globally unique in GCP
  location      = "US"                  # Multi-region for optimal performance in US
  force_destroy = true                  # Allows deletion of bucket even if it contains data

  # Lifecycle rule: Automatically delete files older than 30 days to save costs
  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }
}

# 2. BigQuery Dataset: The Warehouse where dlt/dbt will create tables
resource "google_bigquery_dataset" "rioja_wine_dataset" {
  dataset_id                 = "rioja_wine_data"
  location                   = "US" # Must match the bucket location
  delete_contents_on_destroy = true # Deletes all tables when running 'terraform destroy'
}