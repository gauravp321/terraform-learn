project_id       = "qwiklabs-gcp-00-69e8a21aa75c"	

dataset_id       = "analytics_dataset"
location         = "US"

tables = [
  {
    table_id        = "events_table"
    schema_file     = "schemas/events_table.json"
    partition_field = "event_date"
    partition_type  = "DAY"
    cluster_fields  = ["country", "user_id"]
    description     = "Stores event logs"
  }
]
