terraform {
backend "gcs" {

bucket = "terraform-state-gp" //TODO: Change the bucket name

prefix = "platform/test"

    }
}