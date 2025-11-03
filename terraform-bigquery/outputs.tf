output "dataset_id" {
  description = "BigQuery dataset ID that was created"
  value       = google_bigquery_dataset.dataset.dataset_id
}

output "dataset_location" {
  description = "Location (region) of the BigQuery dataset"
  value       = google_bigquery_dataset.dataset.location
}

output "tables_created" {
  description = "List of BigQuery tables created (table_ids)"
  value       = [for t in google_bigquery_table.tables : t.table_id]
}

output "table_full_names" {
  description = "Full names of the BigQuery tables created (project.dataset.table)"
  value       = [for t in google_bigquery_table.tables :
                  "${t.project}.${t.dataset_id}.${t.table_id}"]
}
