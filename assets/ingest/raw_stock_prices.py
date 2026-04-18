""" @bruin
name: raw_stock_prices
type: python
description: "Fetch daily OHLCV prices from Tiingo API and load into BigQuery raw layer"
image: python:3.11-slim
connection: gcp-tech-stocks

columns:
  - name: date
    type: date
    primary_key: true
  - name: ticker
    type: string
    primary_key: true
  - name: open
    type: float64
  - name: high
    type: float64
  - name: low
    type: float64
  - name: close
    type: float64
  - name: adj_close
    type: float64
  - name: volume
    type: int64
  - name: ingested_at
    type: timestamp

depends: []
@bruin """

import os
import json
import sys
import time
import tempfile
import pandas as pd
from tiingo import TiingoClient
from datetime import datetime
from google.cloud import storage, bigquery
from dotenv import load_dotenv

load_dotenv()

# ── Validation ────────────────────────────────────────────────────────────────

TIINGO_API_KEY = os.getenv("TIINGO_API_KEY")
if not TIINGO_API_KEY:
    raise RuntimeError(
        "ERROR: TIINGO_API_KEY environment variable not set. "
        "Please configure it before running this asset."
    )

BQ_PROJECT = os.getenv("GCP_PROJECT_ID", "tech-stock-analytics")
if not BQ_PROJECT:
    raise RuntimeError(
        "ERROR: GCP_PROJECT_ID environment variable not set. "
        "Please configure it before running this asset."
    )

# ── Config ───────────────────────────────────────────────────────────────────

BQ_DATASET     = "raw_stocks"
BQ_TABLE       = "raw_stock_prices"
GCS_BUCKET     = "tech-stocks-raw"
START_DATE     = "2015-01-01"
END_DATE       = datetime.today().strftime("%Y-%m-%d")

TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA",
    "META", "TSLA", "AMD", "INTC", "ORCL",
]

try:
    client = TiingoClient({"session": True, "api_key": TIINGO_API_KEY})
    bq_client  = bigquery.Client(project=BQ_PROJECT)
    gcs_client = storage.Client(project=BQ_PROJECT)
except Exception as e:
    print(f"ERROR: Failed to initialize clients: {e}", file=sys.stderr)
    raise


# ── Fetch ─────────────────────────────────────────────────────────────────────

def fetch_prices(ticker: str) -> pd.DataFrame:
    print(f"  Fetching {ticker}...")
    raw = client.get_ticker_price(
        ticker,
        startDate=START_DATE,
        endDate=END_DATE,
        frequency="daily",
        fmt="json",
    )
    if not raw:
        print(f"  WARNING: No data for {ticker}")
        return pd.DataFrame()

    df = pd.DataFrame(raw)
    df = df.rename(columns={"adjClose": "adj_close"})
    keep = [c for c in ["date","open","high","low","close","adj_close","volume"] if c in df.columns]
    df = df[keep]
    df["date"]        = pd.to_datetime(df["date"]).dt.date
    df["ticker"]      = ticker
    df["ingested_at"] = datetime.now().isoformat()

    # Cast types
    for col in ["open","high","low","close","adj_close"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("Int64")

    print(f"  {ticker}: {len(df)} rows")
    return df


# ── Upload to GCS ─────────────────────────────────────────────────────────────

def upload_to_gcs(df: pd.DataFrame, ticker: str) -> str:
    run_date = datetime.today().strftime("%Y-%m-%d")
    blob_path = f"prices/{run_date}/{ticker.lower()}_prices.json"

    bucket = gcs_client.bucket(GCS_BUCKET)
    blob   = bucket.blob(blob_path)

    json_data = df.to_json(orient="records", date_format="iso")
    blob.upload_from_string(json_data, content_type="application/json")

    gcs_uri = f"gs://{GCS_BUCKET}/{blob_path}"
    print(f"  Uploaded to {gcs_uri}")
    return gcs_uri


# ── Load to BigQuery ──────────────────────────────────────────────────────────

def load_to_bigquery(df: pd.DataFrame):
    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("date",        "DATE", mode="REQUIRED"),
            bigquery.SchemaField("open",        "FLOAT64"),
            bigquery.SchemaField("high",        "FLOAT64"),
            bigquery.SchemaField("low",         "FLOAT64"),
            bigquery.SchemaField("close",       "FLOAT64"),
            bigquery.SchemaField("adj_close",   "FLOAT64"),
            bigquery.SchemaField("volume",      "INT64"),
            bigquery.SchemaField("ticker",      "STRING", mode="REQUIRED"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
        ],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date",
        ),
        clustering_fields=["ticker"],
    )

    # Deduplicate before load — remove rows already in BQ
    dedup_query = f"""
        SELECT DISTINCT date, ticker
        FROM `{table_ref}`
        WHERE ticker IN ({','.join([f"'{t}'" for t in df['ticker'].unique()])})
    """
    try:
        existing = bq_client.query(dedup_query).to_dataframe()
        if not existing.empty:
            existing["date"] = pd.to_datetime(existing["date"]).dt.date
            before = len(df)
            df = df.merge(existing, on=["date","ticker"], how="left", indicator=True)
            df = df[df["_merge"] == "left_only"].drop(columns=["_merge"])
            print(f"  Dedup: {before} → {len(df)} rows (removed {before - len(df)} existing)")
    except Exception:
        print("  Table empty or first run — skipping dedup")

    if df.empty:
        print("  No new rows to load")
        return

    # Convert date back to string for BQ load
    df["date"] = pd.to_datetime(df["date"])
    df["ingested_at"] = pd.to_datetime(df["ingested_at"])

    job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"  Loaded {len(df)} rows into {table_ref}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\nBruin asset: raw_stock_prices")
    print(f"Run date   : {END_DATE}")
    print(f"Tickers    : {TICKERS}\n")

    try:
        all_frames = []

        for i, ticker in enumerate(TICKERS):
            try:
                df = fetch_prices(ticker)
                if not df.empty:
                    upload_to_gcs(df, ticker)
                    all_frames.append(df)
                time.sleep(0.5)
            except Exception as e:
                print(f"  ERROR processing {ticker}: {e}", file=sys.stderr)
                continue

        if not all_frames:
            print("No data fetched. Exiting.")
            return

        combined = pd.concat(all_frames, ignore_index=True)
        print(f"\nLoading {len(combined):,} total rows to BigQuery...")
        load_to_bigquery(combined)

        print(f"\nDone — raw_stock_prices complete.")
        print(f"Rows loaded : {len(combined):,}")
        print(f"Tickers     : {combined['ticker'].nunique()}")
        print(f"Date range  : {combined['date'].min()} → {combined['date'].max()}")

    except Exception as e:
        print(f"\nFATAL ERROR in raw_stock_prices: {e}", file=sys.stderr)
        raise

if __name__ == "__main__":
    main()
