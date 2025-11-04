output "dataset_id" {
  description = "BigQuery Dataset ID"
  value       = google_bigquery_dataset.dataset.dataset_id
}

output "cloud_function_name" {
  description = "Cloud Function name"
  value       = google_cloudfunctions2_function.gcs_to_bigquery.name
}

output "cloud_function_url" {
  description = "Cloud Function URL"
  value       = google_cloudfunctions2_function.gcs_to_bigquery.service_config[0].uri
}

output "service_account_email" {
  description = "Service Account email for Cloud Function"
  value       = google_service_account.cloud_function_sa.email
}

output "eventarc_trigger_name" {
  description = "Eventarc trigger name"
  value       = google_eventarc_trigger.gcs_trigger.name
}

output "gcs_source_bucket_name" {
  description = "GCS bucket name for source code"
  value       = google_storage_bucket.function_source.name
}

