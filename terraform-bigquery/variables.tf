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


variable "range_partition_start" {
  description = "Start value for range partitioning"
  type        = number
  default     = 0
}

variable "range_partition_end" {
  description = "End value for range partitioning"
  type        = number
  default     = 0
}

variable "range_partition_interval" {
  description = "Interval for range partitioning"
  type        = number
  default     = 0
}

variable "deletion_protection" {
  description = "Enable deletion protection"
  type        = bool
  default     = false
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
    range_partition_start = optional(number)
    range_partition_end = optional(number)
    interval = optional(number)
  }))
}
