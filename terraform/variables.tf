variable "project_id" {
  description = "GCP Project ID"
  type        = string
}


variable "region" {
  description = "GCP Region for resources"
  type        = string
  default     = "us-central1"
}


variable "location" {
  description = "BigQuery Dataset location"
  type        = string
  default     = "US"
}


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



# Cloud Function Variables
variable "cloud_function_name" {
  description = "Cloud Function name"
  type        = string
  default     = "gcs-to-bigquery-loader"
}

variable "sendgrid_api_key" {
  description = "SendGrid API key for sending emails (optional, will log emails if not provided)"
  type        = string
  default     = ""
  sensitive   = true
}

variable "email_enabled" {
  description = "Email flag"
  type        = string
  default     = ""
}

variable "from_email" {
  description = "Email address to send emails from (SendGrid verified sender)"
  type        = string
  default     = ""
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
  default     = "cfdl-gp-test"
}

# Eventarc Variables
variable "eventarc_trigger_name" {
  description = "Eventarc trigger name (optional, defaults to '{cloud_function_name}-trigger')"
  type        = string
  default     = ""
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

