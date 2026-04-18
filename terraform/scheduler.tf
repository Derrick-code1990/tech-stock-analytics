# ── Cloud Scheduler service account ─────────────────────────────────────────
# Scheduler needs its own SA to publish Pub/Sub messages
resource "google_service_account" "scheduler_sa" {
  account_id   = "scheduler-sa"
  display_name = "Cloud Scheduler Service Account"
  project      = var.project_id
}

# ── Pub/Sub topic — Bruin listens here for trigger signal ────────────────────
resource "google_pubsub_topic" "bruin_trigger" {
  name    = "bruin-stock-pipeline-trigger"
  project = var.project_id

  labels = {
    env     = var.environment
    purpose = "pipeline-trigger"
  }
}

# Scheduler SA needs permission to publish to the topic
resource "google_pubsub_topic_iam_member" "scheduler_publisher" {
  topic  = google_pubsub_topic.bruin_trigger.name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.scheduler_sa.email}"
}

# ── Daily stock ingestion job ────────────────────────────────────────────────
resource "google_cloud_scheduler_job" "daily_stock_ingest" {
  name        = "daily-stock-ingest"
  description = "Triggers Bruin stock pipeline Mon–Fri after US market close"
  schedule    = "0 18 * * 1-5"      # 6pm UTC = ~2pm ET — after 4pm ET close with buffer
  time_zone   = "UTC"
  region      = var.region
  project     = var.project_id

  pubsub_target {
    topic_name = google_pubsub_topic.bruin_trigger.id
    data       = base64encode(jsonencode({
      pipeline = "tech-stock-analytics"
      run_date = "{{ today }}"
      tickers  = var.tickers
    }))
  }

  retry_config {
    retry_count          = 3
    max_retry_duration   = "0s"
    min_backoff_duration = "5s"
    max_backoff_duration = "3600s"
    max_doublings        = 5
  }

  depends_on = [
    google_pubsub_topic_iam_member.scheduler_publisher
  ]
}