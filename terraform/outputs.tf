output "gcs_bucket_raw" {
  description = "Raw data lake bucket name"
  value       = google_storage_bucket.raw.name
}

output "gcs_bucket_staged" {
  description = "Staged data lake bucket name"
  value       = google_storage_bucket.staged.name
}

output "gcs_bucket_processed" {
  description = "Processed data lake bucket name"
  value       = google_storage_bucket.processed.name
}

output "gcs_bucket_tf_state" {
  description = "Terraform state bucket name"
  value       = google_storage_bucket.tf_state.name
}

output "bq_dataset_raw" {
  description = "BigQuery raw dataset ID"
  value       = google_bigquery_dataset.raw.dataset_id
}

output "bq_dataset_staging" {
  description = "BigQuery staging dataset ID"
  value       = google_bigquery_dataset.staging.dataset_id
}

output "bq_dataset_marts" {
  description = "BigQuery marts dataset ID"
  value       = google_bigquery_dataset.marts.dataset_id
}

output "bq_table_raw_prices" {
  description = "Fully qualified raw prices table"
  value       = "${var.project_id}.${google_bigquery_dataset.raw.dataset_id}.${google_bigquery_table.raw_stock_prices.table_id}"
}

output "bq_table_mart_ohlcv" {
  description = "Fully qualified mart OHLCV table"
  value       = "${var.project_id}.${google_bigquery_dataset.marts.dataset_id}.${google_bigquery_table.mart_daily_ohlcv.table_id}"
}

output "bq_table_predictions" {
  description = "Fully qualified predictions table"
  value       = "${var.project_id}.${google_bigquery_dataset.marts.dataset_id}.${google_bigquery_table.mart_predictions.table_id}"
}

output "pubsub_topic" {
  description = "Pub/Sub topic for pipeline trigger"
  value       = google_pubsub_topic.bruin_trigger.name
}

output "scheduler_job" {
  description = "Cloud Scheduler job name"
  value       = google_cloud_scheduler_job.daily_stock_ingest.name
}

output "service_account_email" {
  description = "Bruin pipeline service account"
  value       = var.service_account_email
}