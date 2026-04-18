# 📈 Tech Stock Analytics

> An end-to-end batch data pipeline on Google Cloud Platform for ingesting, transforming, and forecasting historical stock prices of the top 10 tech tickers using BigQuery ML.



##  Problem Statement

### The Problem

Retail investors, analysts, and data practitioners who want to track and forecast the performance of major tech stocks face several compounding challenges:

1. **Fragmented, manual data collection** — Historical OHLCV (Open, High, Low, Close, Volume) data for multiple tickers is scattered across APIs and requires custom scripts to retrieve, clean, and store consistently.

2. **No centralized, queryable history** — Without a data warehouse, there is no reliable single source of truth for multi-year stock price history. Ad-hoc CSV exports go stale immediately and cannot be joined with other data sources.

3. **Forecasting requires specialist knowledge** — Building a time-series forecasting model typically requires statistical expertise (ARIMA parameters, seasonality decomposition, etc.) and significant infrastructure to train and serve predictions at scale.

4. **No automated refresh** — Even when a working setup exists, keeping it current with daily market data requires ongoing manual intervention, making it impractical to maintain as a live analytics product.

### How This Project Solves It

**Tech Stock Analytics** is an end-to-end automated data pipeline built on Google Cloud Platform that addresses every one of these pain points:

- **Automated daily ingestion**: A Bruin-orchestrated Python pipeline pulls fresh OHLCV data for 10 major tech tickers (AAPL, MSFT, GOOGL, AMZN, NVDA, META, TSLA, AMD, INTC, ORCL) from the Tiingo API every weekday after US market close. Raw data is staged in Google Cloud Storage and deduplicated before loading into BigQuery, producing a clean, ~28,000-row historical dataset spanning from 2015 to present.

- **A production-grade 3-layer warehouse**: Data flows through a `raw → staging → marts` BigQuery model. The staging layer validates and cleans the data; the marts layer pre-computes moving averages, rolling volatility, cumulative returns, quarterly summaries, and a 30-day Sharpe ratio — all ready for direct querying or visualization.

- **Automated ML forecasting with no manual modelling**: BigQuery ML's `ARIMA_PLUS` model automatically handles parameter selection, holiday adjustment, and spike cleaning. The pipeline trains one model per ticker and generates a 65-day (~1 quarter) price forecast with 90% confidence intervals — without requiring any specialist statistical expertise.

- **A live, self-updating dashboard**: A Looker Studio dashboard connects directly to BigQuery and automatically reflects the latest data after each pipeline run, giving stakeholders an always-current view of historical price trends and forward-looking forecasts.

---

##  Architecture

```
┌──────────────┐     ┌──────────────────┐     ┌──────────────────────┐     ┌────────────────┐
│  Tiingo API  │────▶│  Python (Bruin)  │────▶│  Google Cloud        │────▶│   BigQuery     │
│  (OHLCV data)│     │  Ingest Assets   │     │  Storage (Raw/Staged)│     │   (Warehouse)  │
└──────────────┘     └──────────────────┘     └──────────────────────┘     └───────┬────────┘
                                                                                    │
                      ┌─────────────────────────────────────────────────────────────▼────────┐
                      │                     BigQuery SQL Assets (Bruin)                      │
                      │    raw_stocks  ──▶  staging_stocks  ──▶  marts_stocks               │
                      └──────────────────────────────────────────────────────────────────────┘
                                                                                    │
                                                                          ┌─────────▼────────┐
                                                                          │  BigQuery ML     │
                                                                          │  (ARIMA_PLUS)    │
                                                                          │  Forecast 65 days│
                                                                          └──────────────────┘
```



---

## What It Does

This project builds a **production-style, scheduled batch pipeline** that:

1. **Extracts** ~28,000 rows of daily OHLCV (Open, High, Low, Close, Volume) price data and company metadata for 10 major tech tickers via the **Tiingo API**.
2. **Stages** raw JSON data in **Google Cloud Storage** before loading into BigQuery.
3. **Transforms** raw data through a 3-layer warehouse model (`raw → staging → marts`) using SQL assets managed by **Bruin**.
4. **Forecasts** the next quarter (~65 trading days) of adjusted close prices for each ticker using a **BigQuery ML ARIMA_PLUS** model, outputting confidence intervals to a predictions mart.

---

##  Tech Stack

| Layer | Tool / Service |
|---|---|
| **Ingestion &  Orchestration** | [Bruin](https://github.com/bruin-data/bruin) |
| **Package Manager** | [uv](https://github.com/astral-sh/uv) |
| **Language** | Python 3.11 (Pandas, Requests) |
| **Data Source** | Tiingo REST API |
| **Data Lake** | Google Cloud Storage (GCS) |
| **Data Warehouse** | BigQuery |
| **ML / Forecasting** | BigQuery ML — `ARIMA_PLUS` |
| **Data Vizualization** |Looker Studio (Data Studio) |  
| **Infrastructure** | Terraform & GCP |
| **Dev Environment** | WSL2 (Ubuntu) on Windows |

---

## 🗂️ Project Structure

```
tech-stock-analytics/
├── .bruin.yml                  # Bruin connection config (GCP, DuckDB)
├── pipeline.yml                # Pipeline schedule & global parameters
├── requirements.txt            # Python dependencies
│
├── assets/
│   ├── ingest/                 # Python ingest assets (run by Bruin)
│   │   ├── raw_stock_prices.py
│   │   └── raw_stock_metadata.py
│   ├── staging/                # SQL transformation assets
│   │   ├── stg_stock_prices.sql
│   │   └── stg_stock_metadata.sql
│   └── marts/                  # Business-logic SQL assets & ML
│       ├── bqml_create_models.sql
│       ├── mart_daily_ohlcv.sql
│       ├── mart_predictions.sql
│       ├── mart_quarterly_summary.sql
│       └── mart_stock_returns.sql
│
├── scripts/
│   ├── run_bqml.py             # Run BQML models
│   ├── test_data.pull.py       # Pull test data
│   └── load_test_data.py       # Load test data

├── terraform/                  # IaC for GCP resources                     
```

---

##  Tickers Covered

| Ticker | Company |
|--------|---------|
| AAPL | Apple Inc. |
| MSFT | Microsoft Corporation |
| GOOGL | Alphabet Inc. (Class A) |
| AMZN | Amazon.com Inc. |
| NVDA | NVIDIA Corp. |
| META | Meta Platforms Inc. |
| TSLA | Tesla Inc. |
| AMD | Advanced Micro Devices |
| INTC | Intel Corp. |
| ORCL | Oracle Corp. |

---

## ⚙️ Setup & Installation

### Prerequisites

- Python 3.11+
- [`uv`](https://docs.astral.sh/uv/) installed
- [Bruin CLI](https://bruin-data.github.io/bruin/) installed
- A Google Cloud project with BigQuery and GCS enabled
- A GCP Service Account key with BigQuery Admin and Storage Admin roles

### 1. Clone the Repository

```bash
git clone https://github.com/<your-username>/tech-stock-analytics.git
cd tech-stock-analytics
```

### 2. Install Dependencies

```bash
# Create and sync the virtual environment using uv
uv venv
uv pip install -r requirements.txt
source .venv/bin/activate
```

### 3. Configure Environment Variables

Copy the example env file and fill in your values:

```bash
cp .env.example .env
```

Edit `.env`:

```dotenv
# GCP
GCP_PROJECT_ID= ------
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service-account-key.json

# Tiingo API
TIINGO_API_KEY= -------

# GCS
GCS_BUCKET_RAW= ------
GCS_BUCKET_STAGED= ------
```

### 4. Configure Bruin

Edit `.bruin.yml` and update the `service_account_file` path and `project_id` to match your environment:

```yaml
environments:
  default:
    connections:
      google_cloud_platform:
        - name: gcp-tech-stocks
          project_id: ------
          service_account_file: /path/to/bruin-sa-key.json
          location: US
```

### 5. (Optional) Provision GCP Resources with Terraform

```bash
cd terraform/
terraform init
terraform plan
terraform apply
```

---

## 🚀 Running the Pipeline

### Full Pipeline Run (via Bruin)

To trigger the pipeline :

```bash
# Validate all assets
bruin validate .

# Run the full pipeline
bruin run .
```
### Run BQML Forecasting Standalone

After the pipeline has populated `marts_stocks.mart_daily_ohlcv`, you can train ARIMA_PLUS models and generate forecasts directly:

```bash
python3 scripts/run_bqml.py
```

This will:
- Train one `ARIMA_PLUS` model per ticker in `ml_stocks.*`
- Generate a 65-day (≈1 quarter) price forecast with 90% confidence intervals
- Write 650 rows into `marts_stocks.mart_predictions`
- Print a summary table with AIC / Log-Likelihood model quality metrics

---

##  BigQuery Dataset Layout

| Dataset | Description |
|---|---|
| `raw_stocks` | Raw ingested data from Tiingo (prices & metadata) |
| `staging_stocks` | Cleaned, deduplicated, and validated staging tables |
| `marts_stocks` | Business-ready aggregated tables and BQML predictions |
| `ml_stocks` | Trained ARIMA_PLUS models (one per ticker) |

---

## Forecast Output

The final predictions table (`marts_stocks.mart_predictions`) contains:

| Column | Type | Description |
|---|---|---|
| `ticker` | STRING | Stock ticker symbol |
| `forecast_date` | DATE | Future trading date |
| `predicted_close` | FLOAT64 | Forecasted adjusted close price |
| `prediction_interval_lower_value` | FLOAT64 | Lower bound of 90% confidence interval |
| `prediction_interval_upper_value` | FLOAT64 | Upper bound of 90% confidence interval |
| `confidence_level` | FLOAT64 | Confidence level (0.90) |
| `model_run_date` | DATE | Date the forecast was generated |

---

## 📊 Live Dashboard

The Looker Studio dashboard visualizes the pipeline output in real-time, directly connected to BigQuery.

**[→ Open Dashboard](https://datastudio.google.com/reporting/18a7d0ac-6ab2-461d-8d9e-79c94168d378/page/izavF)**

| Tile | Source Table | Description |
|---|---|---|
| Historical Price & Moving Averages | `marts_stocks.mart_daily_ohlcv` | Adj. close with MA-7, MA-30, MA-90 overlays |
| Price Forecast vs. History | `marts_stocks.v_history_and_forecast` | BQML ARIMA_PLUS 65-day forecast with 90% confidence bands |

---

## ✅ Data Quality

Data validation is enforced at multiple layers:

- **Ingest**: Deduplication logic in `raw_stock_prices.py` prevents re-loading existing rows.
- **Staging**: SQL filters remove null tickers, zero/negative prices, and invalid volume data.
- **Marts**: Row-level `is_valid` flag in `mart_daily_ohlcv` tracks data quality per trading day.
