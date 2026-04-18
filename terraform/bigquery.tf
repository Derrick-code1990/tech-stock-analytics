# ── Datasets ────────────────────────────────────────────────────────────────

resource "google_bigquery_dataset" "raw" {
  dataset_id                  = "raw_stocks"
  friendly_name               = "Raw Stocks"
  description                 = "Raw ingested data from Tiingo API via Bruin"
  location                    = var.bq_location
  delete_contents_on_destroy  = false

  labels = {
    env   = var.environment
    layer = "raw"
  }
}

resource "google_bigquery_dataset" "staging" {
  dataset_id                  = "staging_stocks"
  friendly_name               = "Staging Stocks"
  description                 = "Cleaned, typed and deduplicated stock data"
  location                    = var.bq_location
  delete_contents_on_destroy  = false

  labels = {
    env   = var.environment
    layer = "staging"
  }
}

resource "google_bigquery_dataset" "marts" {
  dataset_id                  = "marts_stocks"
  friendly_name               = "Marts Stocks"
  description                 = "Business-ready aggregates, technical indicators, and ML predictions"
  location                    = var.bq_location
  delete_contents_on_destroy  = false

  labels = {
    env   = var.environment
    layer = "marts"
  }
}

# ── Raw tables ──────────────────────────────────────────────────────────────

resource "google_bigquery_table" "raw_stock_prices" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "raw_stock_prices"
  deletion_protection = false
  description         = "Raw daily OHLCV prices loaded from Tiingo"

  time_partitioning {
    type  = "DAY"
    field = "date"
  }

  clustering = ["ticker"]

  schema = jsonencode([
    { name = "date",         type = "DATE",      mode = "REQUIRED", description = "Trading date" },
    { name = "ticker",       type = "STRING",    mode = "REQUIRED", description = "Stock ticker symbol" },
    { name = "open",         type = "FLOAT64",   mode = "NULLABLE", description = "Opening price" },
    { name = "high",         type = "FLOAT64",   mode = "NULLABLE", description = "Daily high price" },
    { name = "low",          type = "FLOAT64",   mode = "NULLABLE", description = "Daily low price" },
    { name = "close",        type = "FLOAT64",   mode = "NULLABLE", description = "Closing price" },
    { name = "adj_close",    type = "FLOAT64",   mode = "NULLABLE", description = "Adjusted closing price" },
    { name = "volume",       type = "INT64",     mode = "NULLABLE", description = "Daily trading volume" },
    { name = "ingested_at",  type = "TIMESTAMP", mode = "NULLABLE", description = "UTC timestamp of ingestion" },
  ])

  labels = {
    env   = var.environment
    layer = "raw"
  }
}

resource "google_bigquery_table" "raw_stock_metadata" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "raw_stock_metadata"
  deletion_protection = false
  description         = "Company metadata for each tracked ticker"

  schema = jsonencode([
    { name = "ticker",       type = "STRING",    mode = "REQUIRED", description = "Stock ticker symbol" },
    { name = "company_name", type = "STRING",    mode = "NULLABLE", description = "Full company name" },
    { name = "description",  type = "STRING",    mode = "NULLABLE", description = "Company description" },
    { name = "exchange",     type = "STRING",    mode = "NULLABLE", description = "Stock exchange" },
    { name = "start_date",   type = "STRING",    mode = "NULLABLE", description = "Earliest available date" },
    { name = "end_date",     type = "STRING",    mode = "NULLABLE", description = "Latest available date" },
    { name = "fetched_at",   type = "TIMESTAMP", mode = "NULLABLE", description = "UTC timestamp of fetch" },
  ])

  labels = {
    env   = var.environment
    layer = "raw"
  }
}

# ── Staging tables ──────────────────────────────────────────────────────────

resource "google_bigquery_table" "stg_stock_prices" {
  dataset_id          = google_bigquery_dataset.staging.dataset_id
  table_id            = "stg_stock_prices"
  deletion_protection = false
  description         = "Cleaned, deduplicated daily prices with derived columns"

  time_partitioning {
    type  = "DAY"
    field = "date"
  }

  clustering = ["ticker"]

  schema = jsonencode([
    { name = "date",           type = "DATE",    mode = "REQUIRED" },
    { name = "ticker",         type = "STRING",  mode = "REQUIRED" },
    { name = "open",           type = "FLOAT64", mode = "NULLABLE" },
    { name = "high",           type = "FLOAT64", mode = "NULLABLE" },
    { name = "low",            type = "FLOAT64", mode = "NULLABLE" },
    { name = "close",          type = "FLOAT64", mode = "NULLABLE" },
    { name = "adj_close",      type = "FLOAT64", mode = "NULLABLE" },
    { name = "volume",         type = "INT64",   mode = "NULLABLE" },
    { name = "daily_return",   type = "FLOAT64", mode = "NULLABLE", description = "(close - prev_close) / prev_close" },
    { name = "price_range",    type = "FLOAT64", mode = "NULLABLE", description = "high - low" },
    { name = "is_valid",       type = "BOOL",    mode = "NULLABLE", description = "Passes data quality checks" },
  ])

  labels = {
    env   = var.environment
    layer = "staging"
  }
}

# ── Marts tables ─────────────────────────────────────────────────────────────

resource "google_bigquery_table" "mart_daily_ohlcv" {
  dataset_id          = google_bigquery_dataset.marts.dataset_id
  table_id            = "mart_daily_ohlcv"
  deletion_protection = false
  description         = "Clean daily OHLCV — primary table for Looker Studio"

  time_partitioning {
    type  = "DAY"
    field = "date"
  }

  clustering = ["ticker"]

  schema = jsonencode([
    { name = "date",        type = "DATE",    mode = "REQUIRED" },
    { name = "ticker",      type = "STRING",  mode = "REQUIRED" },
    { name = "open",        type = "FLOAT64", mode = "NULLABLE" },
    { name = "high",        type = "FLOAT64", mode = "NULLABLE" },
    { name = "low",         type = "FLOAT64", mode = "NULLABLE" },
    { name = "close",       type = "FLOAT64", mode = "NULLABLE" },
    { name = "adj_close",   type = "FLOAT64", mode = "NULLABLE" },
    { name = "volume",      type = "INT64",   mode = "NULLABLE" },
    { name = "daily_return",type = "FLOAT64", mode = "NULLABLE" },
    { name = "ma_7",        type = "FLOAT64", mode = "NULLABLE", description = "7-day moving average" },
    { name = "ma_30",       type = "FLOAT64", mode = "NULLABLE", description = "30-day moving average" },
    { name = "ma_90",       type = "FLOAT64", mode = "NULLABLE", description = "90-day moving average" },
    { name = "volatility_30", type = "FLOAT64", mode = "NULLABLE", description = "30-day rolling std dev" },
  ])

  labels = {
    env   = var.environment
    layer = "marts"
  }
}

resource "google_bigquery_table" "mart_quarterly_summary" {
  dataset_id          = google_bigquery_dataset.marts.dataset_id
  table_id            = "mart_quarterly_summary"
  deletion_protection = false
  description         = "Quarterly aggregates per ticker for trend analysis"

  schema = jsonencode([
    { name = "ticker",              type = "STRING",  mode = "REQUIRED" },
    { name = "year",                type = "INT64",   mode = "REQUIRED" },
    { name = "quarter",             type = "INT64",   mode = "REQUIRED" },
    { name = "quarter_start_date",  type = "DATE",    mode = "NULLABLE" },
    { name = "quarter_end_date",    type = "DATE",    mode = "NULLABLE" },
    { name = "open_price",          type = "FLOAT64", mode = "NULLABLE" },
    { name = "close_price",         type = "FLOAT64", mode = "NULLABLE" },
    { name = "high_price",          type = "FLOAT64", mode = "NULLABLE" },
    { name = "low_price",           type = "FLOAT64", mode = "NULLABLE" },
    { name = "avg_volume",          type = "FLOAT64", mode = "NULLABLE" },
    { name = "quarterly_return_pct",type = "FLOAT64", mode = "NULLABLE" },
    { name = "max_drawdown",        type = "FLOAT64", mode = "NULLABLE" },
    { name = "trading_days",        type = "INT64",   mode = "NULLABLE" },
  ])

  labels = {
    env   = var.environment
    layer = "marts"
  }
}

resource "google_bigquery_table" "mart_predictions" {
  dataset_id          = google_bigquery_dataset.marts.dataset_id
  table_id            = "mart_predictions"
  deletion_protection = false
  description         = "BQML ARIMA_PLUS forecast output — next quarter predictions"

  schema = jsonencode([
    { name = "ticker",              type = "STRING",  mode = "REQUIRED" },
    { name = "forecast_date",       type = "DATE",    mode = "REQUIRED" },
    { name = "predicted_close",     type = "FLOAT64", mode = "NULLABLE" },
    { name = "prediction_interval_lower_value", type = "FLOAT64", mode = "NULLABLE" },
    { name = "prediction_interval_upper_value", type = "FLOAT64", mode = "NULLABLE" },
    { name = "confidence_level",    type = "FLOAT64", mode = "NULLABLE" },
    { name = "model_run_date",      type = "DATE",    mode = "NULLABLE" },
  ])

  labels = {
    env   = var.environment
    layer = "marts"
  }
}

resource "google_bigquery_table" "mart_stock_returns" {
  dataset_id          = google_bigquery_dataset.marts.dataset_id
  table_id            = "mart_stock_returns"
  deletion_protection = false
  description         = "Daily, weekly, monthly and cumulative return metrics"

  time_partitioning {
    type  = "DAY"
    field = "date"
  }

  clustering = ["ticker"]

  schema = jsonencode([
    { name = "date",             type = "DATE",    mode = "REQUIRED" },
    { name = "ticker",           type = "STRING",  mode = "REQUIRED" },
    { name = "adj_close",        type = "FLOAT64", mode = "NULLABLE" },
    { name = "daily_return",     type = "FLOAT64", mode = "NULLABLE" },
    { name = "weekly_return",    type = "FLOAT64", mode = "NULLABLE" },
    { name = "monthly_return",   type = "FLOAT64", mode = "NULLABLE" },
    { name = "quarterly_return", type = "FLOAT64", mode = "NULLABLE" },
    { name = "ytd_return",       type = "FLOAT64", mode = "NULLABLE" },
    { name = "cumulative_return",type = "FLOAT64", mode = "NULLABLE" },
    { name = "sharpe_ratio_30d", type = "FLOAT64", mode = "NULLABLE" },
    { name = "is_up_day",        type = "INT64",   mode = "NULLABLE" },
    { name = "win_rate_30d",     type = "FLOAT64", mode = "NULLABLE" },
  ])

  labels = {
    env   = var.environment
    layer = "marts"
  }
}

# ── ML Dataset ──────────────────────────────────────────────────────────────

resource "google_bigquery_dataset" "ml" {
  dataset_id                 = "ml_stocks"
  friendly_name              = "ML Stocks"
  description                = "BQML models for stock price forecasting"
  location                   = var.bq_location
  delete_contents_on_destroy = false

  labels = {
    env   = var.environment
    layer = "ml"
  }
}

resource "google_bigquery_dataset_iam_member" "ml_data_editor" {
  dataset_id = google_bigquery_dataset.ml.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${var.service_account_email}"
}