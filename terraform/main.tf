terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}


# BigQuery Dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id  = "analytics"
  location    = var.location
  description = "Centralized dataset for analytics"
}



#---------------------------------old code----------------------------------------------------

/*
resource "google_bigquery_table" "tables" {
  for_each = { for t in var.tables : t.table_id => t }

  dataset_id  = google_bigquery_dataset.dataset.dataset_id
  table_id    = each.value.table_id
  description = lookup(each.value, "description", null)

  schema = file("${path.module}/../schemas/${each.value.schema_file}")

  dynamic "time_partitioning" {
    for_each = each.value.partition_type == "TIME" ? [1] : []
    content {
      type  = "DAY"
      field = each.value.partition_field
    }
  }

  # Range partitioning (for INTEGER/DATE fields)
  dynamic "range_partitioning" {
    for_each = each.value.partition_field == "RANGE" ? [1] : []
    content {
      field = each.value.partition_field
      range {
        start    = each.value.range_partition_start
        end      = each.value.range_partition_end
        interval = each.value.range_partition_interval
      }
    }
  }

  clustering = length(each.value.cluster_fields) > 0 ? each.value.cluster_fields : null

}

*/
#------------------------------------old code-------------------------------------------------


#------------------------------------new code-------------------------------------------------

# Read the SQL file from your repo
locals {
  bq_sql = templatefile("${path.module}/../schemas/tables.sql",
    {project_id = var.project_id}
  )
}

resource "google_bigquery_job" "create_tables" {
  job_id   = "create-bq-tables-${formatdate("YYYYMMDDhhmmss", timestamp())}"
  project  = var.project_id
  location = var.region

  query {
    query          = local.bq_sql
    use_legacy_sql = false
  }
}

#------------------------------------new code-------------------------------------------------


# Service Account to create Cloud Function
data "google_service_account" "cloud_function_sa" {
  account_id   = var.service_account_name
}

# SA for cloud function
data "google_service_account" "cloud_function_sa_use" {
  account_id   = "cloud-function-sa"
}

# IAM bindings for the service account
# resource "google_project_iam_member" "bigquery_data_editor" {
#   project = var.project_id
#   role    = "roles/bigquery.dataEditor"
#   member  = "serviceAccount:${google_service_account.cloud_function_sa.email}"
# }

# resource "google_project_iam_member" "bigquery_job_user" {
#   project = var.project_id
#   role    = "roles/bigquery.jobUser"
#   member  = "serviceAccount:${google_service_account.cloud_function_sa.email}"
# }


# resource "google_project_iam_member" "storage_object_viewer" {
#   project = var.project_id
#   role    = "roles/storage.objectViewer"
#   member  = "serviceAccount:${google_service_account.cloud_function_sa.email}"
# }

resource "google_storage_bucket" "trigger-bucket" {
  name                        = "eventarc-gp74"
  location                    = "us-central1" # The trigger must be in the same location as the bucket
  uniform_bucket_level_access = true
}

# Cloud Storage Bucket for Cloud Function source code
resource "google_storage_bucket" "function_source" {
  name          = "${var.project_id}-cloud-function-source-${random_id.bucket_suffix.hex}"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

}

# Random ID for bucket suffix
resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# Local values for computed names
locals {
  eventarc_trigger_name = var.eventarc_trigger_name != "" ? var.eventarc_trigger_name : "${var.cloud_function_name}-trigger"
}


# Archive Cloud Function source code
data "archive_file" "function_source" {
  type        = "zip"
  source_dir  = "${path.module}/../cloud-function"
  output_path = "${path.module}/function_source.zip"
}


# Upload source code to GCS
resource "google_storage_bucket_object" "function_source" {
  name   = "function_source_${data.archive_file.function_source.output_md5}.zip"
  bucket = google_storage_bucket.function_source.name
  source = data.archive_file.function_source.output_path
}


# Cloud Function
resource "google_cloudfunctions2_function" "gcs_to_bigquery" {
  name        = var.cloud_function_name
  location    = var.region
  description = "Cloud Function to load files from GCS to BigQuery"

  build_config {
    runtime     = "python311"
    entry_point = "process_config_file"
    source {
      storage_source {
        bucket = google_storage_bucket.function_source.name
        object = google_storage_bucket_object.function_source.name
      }
    }
  }

  service_config {
    max_instance_count    = var.cloud_function_max_instances
    min_instance_count    = var.cloud_function_min_instances
    available_memory      = var.cloud_function_memory
    timeout_seconds       = var.cloud_function_timeout
    service_account_email = data.google_service_account.cloud_function_sa_use.email
    environment_variables = {
      SENDGRID_API_KEY   = var.sendgrid_api_key != "" ? var.sendgrid_api_key : ""
      FROM_EMAIL         = var.from_email != "" ? var.from_email : ""
    }
  }
}


# Wait for Cloud Function to be fully deployed (Cloud Run service needs to be ready)
resource "time_sleep" "wait_for_cloud_function" {
  depends_on = [google_cloudfunctions2_function.gcs_to_bigquery]
  create_duration = "30s"
}


# Eventarc Trigger for GCS Object Finalize Events Note: Eventarc for GCS requires Pub/Sub transport

resource "google_eventarc_trigger" "gcs_trigger" {
  name     = google_storage_bucket.trigger-bucket.name
  location = var.region

  matching_criteria {
    attribute = "type"
    value     = "google.cloud.storage.object.v1.finalized"
  }

  matching_criteria {
    attribute = "bucket"
    value     = google_storage_bucket.trigger-bucket.name
  }


  service_account = data.google_service_account.cloud_function_sa_use.email
  
  destination {
    cloud_run_service {
      #service = google_cloudfunctions2_function.gcs_to_bigquery.name
      service = google_cloudfunctions2_function.gcs_to_bigquery.service_config[0].service
      region  = var.region
    }
  }

  depends_on = [
    # google_pubsub_topic_iam_member.eventarc_publisher,
    # google_cloudfunctions2_function_iam_member.eventarc_invoker
    google_cloudfunctions2_function.gcs_to_bigquery
  ]
}


# Pub/Sub Topic for Eventarc transport
# resource "google_pubsub_topic" "gcs_events" {
#   name = var.pubsub_topic_name
# }


# IAM binding for Eventarc service agent to publish to Pub/Sub
# resource "google_pubsub_topic_iam_member" "eventarc_publisher" {
#   topic  = google_pubsub_topic.gcs_events.id
#   role   = "roles/pubsub.publisher"
#   member = "serviceAccount:service-${data.google_project.project.number}@gs-project-accounts.iam.gserviceaccount.com"
# }

# IAM binding for Eventarc to invoke Cloud Function
# resource "google_cloudfunctions2_function_iam_member" "eventarc_invoker" {
#   project        = var.project_id
#   location       = var.region
#   cloud_function = google_cloudfunctions2_function.gcs_to_bigquery.name
#   role           = "roles/cloudfunctions.invoker"
#   member         = "serviceAccount:${google_service_account.cloud_function_sa.email}"
# }

# Grant Eventarc service account necessary permissions
# resource "google_project_iam_member" "eventarc_service_agent" {
#   project = var.project_id
#   role    = "roles/eventarc.serviceAgent"
#   member  = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-eventarc.iam.gserviceaccount.com"
# }

data "google_project" "project" {
  project_id = var.project_id
}
