variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "Default GCP region"
  type        = string
  default     = "us-east1"
}

variable "zone" {
  description = "Default GCP zone"
  type        = string
  default     = "us-east1-b"
}

variable "bq_location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "US"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
}

variable "service_account_email" {
  description = "Bruin pipeline service account email"
  type        = string
}

variable "data_retention_days" {
  description = "GCS object lifecycle retention in days"
  type        = number
  default     = 365
}

variable "tickers" {
  description = "List of stock tickers to track"
  type        = list(string)
  default     = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "AMD", "INTC", "ORCL"]
}