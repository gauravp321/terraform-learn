variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "credentials_file" {
  description = "Path to the GCP service account key file"
  type        = string
}

variable "location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "US"
}

variable "dataset_id" {
  description = "BigQuery dataset ID"
  type        = string
}

variable "tables" {
  description = "List of BigQuery table definitions"
  type = list(object({
    table_id        = string
    description     = optional(string)
    schema_file     = string
    partition_field = optional(string)
    partition_type  = optional(string)
    cluster_fields  = optional(list(string))
  }))
}
