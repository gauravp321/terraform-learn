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
  # dynamic "time_partitioning" {
  #   for_each = each.value.partition_field != null ? [1] : []
  #   content {
  #     type  = lookup(each.value, "partition_type", "DAY")
  #     field = each.value.partition_field
  #   }
  # }

  dynamic "time_partitioning" {
    for_each = each.value.partition_field == "TIME" ? [1] : []
    content {
      type                     = "DAY"
      field                    = each.value.partition_field
      expiration_ms            = each.value.partition_expiration_ms > 0 ? ach.value.partition_expiration_ms : null
    }
  }

  # Range partitioning (for INTEGER/DATE fields)
  dynamic "range_partitioning" {
    for_each = each.value.partition_field == "RANGE" ? [1] : []
    content {
      field                    = each.value.partition_field
      range {
        start                  = each.value.range_partition_start
        end                    = each.value.range_partition_end
        interval               = each.value.range_partition_interval
      }
    }
  }

  clustering = length(each.value.cluster_fields) > 0 ? each.value.cluster_fields : null

}
