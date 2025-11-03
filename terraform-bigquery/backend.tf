terraform {
  backend "gcs" {
    # Name of the GCS bucket where Terraform state will be stored
    bucket = "sample-tf1"

    # Prefix (path inside the bucket) to keep state files tidy
    prefix = "bigquery/terraform.tfstate"

  }
}
