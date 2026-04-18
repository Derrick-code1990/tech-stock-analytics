terraform {
  required_version = ">= 1.6.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  # Remote state stored in GCS — prevents state loss
  # Comment this block out on FIRST run, then uncomment after bucket exists
  backend "gcs" {
    bucket = "tech-stocks-tf-state"
    prefix = "terraform/state"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}