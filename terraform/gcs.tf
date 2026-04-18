# ── Terraform state bucket ─────────────────────────────────────────────────
resource "google_storage_bucket" "tf_state" {
  name          = "tech-stocks-tf-state"
  location      = var.region
  force_destroy = false

  versioning {
    enabled = true
  }

  uniform_bucket_level_access = true

  labels = {
    env     = var.environment
    purpose = "terraform-state"
  }
}

# ── Raw zone ────────────────────────────────────────────────────────────────
# Stores exactly what comes out of the API — JSON, untouched
resource "google_storage_bucket" "raw" {
  name          = "tech-stocks-raw"
  location      = var.region
  force_destroy = false

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = var.data_retention_days
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  uniform_bucket_level_access = true

  labels = {
    env   = var.environment
    layer = "raw"
  }
}

# ── Staged zone ─────────────────────────────────────────────────────────────
# Cleaned, typed, Parquet-converted data
resource "google_storage_bucket" "staged" {
  name          = "tech-stocks-staged"
  location      = var.region
  force_destroy = false

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = var.data_retention_days
    }
    action {
      type = "Delete"
    }
  }

  uniform_bucket_level_access = true

  labels = {
    env   = var.environment
    layer = "staged"
  }
}

# ── Processed zone ──────────────────────────────────────────────────────────
# Final aggregates ready for BigQuery external tables or direct load
resource "google_storage_bucket" "processed" {
  name          = "tech-stocks-processed"
  location      = var.region
  force_destroy = false

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = var.data_retention_days
    }
    action {
      type = "Delete"
    }
  }

  uniform_bucket_level_access = true

  labels = {
    env   = var.environment
    layer = "processed"
  }
}

# ── Folder structure (placeholder objects act as directory markers) ─────────
resource "google_storage_bucket_object" "raw_prices_folder" {
  name    = "prices/.keep"
  bucket  = google_storage_bucket.raw.name
  content = " "
}

resource "google_storage_bucket_object" "raw_metadata_folder" {
  name    = "metadata/.keep"
  bucket  = google_storage_bucket.raw.name
  content = " "
}

resource "google_storage_bucket_object" "staged_prices_folder" {
  name    = "prices/.keep"
  bucket  = google_storage_bucket.staged.name
  content = " "
}

resource "google_storage_bucket_object" "processed_folder" {
  name    = "marts/.keep"
  bucket  = google_storage_bucket.processed.name
  content = " "
}