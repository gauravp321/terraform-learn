terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
  required_version = ">= 1.5.0"

  # (Optional) Use a GCS bucket for remote state storage — uncomment if needed
  # backend "gcs" {
  #   bucket = "your-terraform-state-bucket"
  #   prefix = "bigquery/state"
  # }
}

provider "google" {
  project     = var.project_id
  region      = var.location
  credentials = file(var.credentials_file)
}

# BigQuery Dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id  = var.dataset_id
  location    = var.location
  description = "Centralized dataset for analytics"
}

# BigQuery Tables (Dynamic - supports partitioning and clustering)
resource "google_bigquery_table" "tables" {
  for_each = { for t in var.tables : t.table_id => t }

  dataset_id  = google_bigquery_dataset.dataset.dataset_id
  table_id    = each.value.table_id
  description = lookup(each.value, "description", null)

  schema = file("${path.module}/${each.value.schema_file}")

  # Conditional Partitioning
  dynamic "time_partitioning" {
    for_each = each.value.partition_field != null ? [1] : []
    content {
      type  = lookup(each.value, "partition_type", "DAY")
      field = each.value.partition_field
    }
  }

  # Conditional Clustering
  dynamic "clustering" {
    for_each = length(lookup(each.value, "cluster_fields", [])) > 0 ? [1] : []
    content {
      fields = each.value.cluster_fields
    }
  }
}
