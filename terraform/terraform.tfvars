project_id = "qwiklabs-gcp-04-775b17c0474f"
location   = "US"

tables = [
  {
    table_id        = "events_table"
    schema_file     = "events_table.json"
    partition_field = "event_date"
    partition_type  = "TIME"
    cluster_fields  = ["country", "user_id"]
    description     = "Stores event logs"
  },

  {
    table_id       = "users_table"
    schema_file    = "users_table.json"
    cluster_fields = []
    description    = "User information"
  },

  {
    table_id        = "sales_table"
    schema_file     = "sales_table.json"
    partition_field = "sale_date"
    partition_type  = "TIME"
    cluster_fields  = ["region"]
  }

]

cloud_function_name   = "gcs_bq_cf_deploy"
cloud_function_memory = "512Mi"
