variable "project_id" {
  description = "GCP Project ID"
  type        = string
}


variable "region" {
  description = "GCP Region for resources"
  type        = string
  default     = "us-east1"
}



variable "location" {
  description = "BigQuery Dataset location"
  type        = string
  default     = "US"
}

# variable "default_table_expiration_ms" {
#   description = "Default table expiration in milliseconds"
#   type        = number
#   default     = null
# }

# variable "default_partition_expiration_ms" {
#   description = "Default partition expiration in milliseconds"
#   type        = number
#   default     = 2592000000 # 30 days
# }



variable "tables" {
  description = "List of BigQuery table definitions"
  type = list(object({
    table_id              = string
    description           = optional(string)
    schema_file           = string
    partition_field       = optional(string)
    partition_type        = optional(string)
    cluster_fields        = optional(list(string))
    range_partition_start = optional(number)
    range_partition_end   = optional(number)
    interval              = optional(number)
  }))
}


# variable "partition_type" {
#   description = "Partition type for BigQuery table (DAY, HOUR, MONTH, YEAR)"
#   type        = string
#   default     = "DAY"
# }

# variable "partition_field" {
#   description = "Field name for partitioning"
#   type        = string
#   default     = "load_timestamp"
# }

# variable "partition_field_type" {
#   description = "Data type for partition field (TIMESTAMP, DATE, INTEGER)"
#   type        = string
#   default     = "TIMESTAMP"
# }

# variable "partition_expiration_ms" {
#   description = "Partition expiration in milliseconds"
#   type        = number
#   default     = 2592000000 # 30 days
# }

# variable "require_partition_filter" {
#   description = "Require partition filter for queries"
#   type        = bool
#   default     = false
# }

# variable "clustering_fields" {
#   description = "List of fields for clustering (bucketing)"
#   type        = list(string)
#   default     = ["data_column_1"]
# }

# Cloud Function Variables
variable "cloud_function_name" {
  description = "Cloud Function name"
  type        = string
  default     = "gcs-to-bigquery-loader"
}

variable "cloud_function_max_instances" {
  description = "Maximum number of Cloud Function instances"
  type        = number
  default     = 10
}

variable "cloud_function_min_instances" {
  description = "Minimum number of Cloud Function instances"
  type        = number
  default     = 0
}

variable "cloud_function_memory" {
  description = "Memory allocation for Cloud Function (256Mi, 512Mi, 1Gi, 2Gi, 4Gi, 8Gi)"
  type        = string
  default     = "512Mi"
}

variable "cloud_function_timeout" {
  description = "Cloud Function timeout in seconds"
  type        = number
  default     = 540
}

variable "service_account_name" {
  description = "Service account name for Cloud Function"
  type        = string
  default     = "cf-bq-loader-sa"
}

#cf-bq-loader-sa@qwiklabs-gcp-03-befd9ad9ff76.iam.gserviceaccount.com
# Eventarc Variables
variable "eventarc_trigger_name" {
  description = "Eventarc trigger name"
  type        = string
  default     = "gcs-to-bigquery-trigger"
}


variable "gcs_file_prefix" {
  description = "Optional GCS file prefix filter (e.g., 'uploads/')"
  type        = string
  default     = ""
}

variable "pubsub_topic_name" {
  description = "Pub/Sub topic name for Eventarc"
  type        = string
  default     = "gcs-events"
}

variable "schema_bucket" {
  description = "GCS bucket name where schema files are stored (optional, defaults to gcs_source_bucket)"
  type        = string
  default     = ""
}

variable "schema_path" {
  description = "Path prefix in bucket where schema files are stored"
  type        = string
  default     = "schemas"
}

variable "example_table_schema_file" {
  description = "Schema file name for example table (e.g., 'example_table_schema.json') from schemas folder"
  type        = string
  default     = ""
}

