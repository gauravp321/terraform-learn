project_id = "quantiphi-test-470710"
location   = "US"

tables = [
  {
    table_id        = "eventss_table"
    schema_file     = "eventss_table.json"
    partition_field = "event_date"
    partition_type  = "TIME"
    cluster_fields  = ["country", "user_id"]
    description     = "Stores event logs"
  },

  {
    table_id       = "userss_table"
    schema_file    = "userss_table.json"
    cluster_fields = []
    description    = "User information"
  },

  {
    table_id        = "saless_table"
    schema_file     = "saless_table.json"
    partition_field = "sale_date"
    partition_type  = "TIME"
    cluster_fields  = ["region"]
  }

]

cloud_function_name   = "gcs_bq_cf_deploy"
cloud_function_memory = "512Mi"
