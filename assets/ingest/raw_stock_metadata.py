""" @bruin
name: raw_stock_metadata
type: python
description: "Fetch company metadata from Tiingo and load into BigQuery raw layer"
image: python:3.11-slim
connection: gcp-tech-stocks

columns:
  - name: ticker
    type: string
    primary_key: true
  - name: company_name
    type: string
  - name: description
    type: string
  - name: exchange
    type: string
  - name: start_date
    type: string
  - name: end_date
    type: string
  - name: fetched_at
    type: timestamp

depends: []
@bruin """

import os
import sys
import time
import pandas as pd
from tiingo import TiingoClient
from datetime import datetime
from google.cloud import bigquery
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

BQ_DATASET     = "raw_stocks"
BQ_TABLE       = "raw_stock_metadata"

TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA",
    "META", "TSLA", "AMD", "INTC", "ORCL",
]

try:
    client    = TiingoClient({"session": True, "api_key": TIINGO_API_KEY})
    bq_client = bigquery.Client(project=BQ_PROJECT)
except Exception as e:
    print(f"ERROR: Failed to initialize clients: {e}", file=sys.stderr)
    raise


def fetch_metadata(ticker: str) -> dict:
    print(f"  Fetching metadata: {ticker}...")
    try:
        meta = client.get_ticker_metadata(ticker)
        return {
            "ticker":       meta.get("ticker", ticker).upper(),
            "company_name": meta.get("name", ""),
            "description":  (meta.get("description") or "")[:300],
            "exchange":     meta.get("exchangeCode", ""),
            "start_date":   meta.get("startDate", ""),
            "end_date":     meta.get("endDate", ""),
            "fetched_at":   datetime.now().isoformat(),
        }
    except Exception as e:
        print(f"  ERROR {ticker}: {e}")
        return {"ticker": ticker, "fetched_at": datetime.now().isoformat()}


def load_to_bigquery(df: pd.DataFrame):
    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("ticker",       "STRING"),
            bigquery.SchemaField("company_name", "STRING"),
            bigquery.SchemaField("description",  "STRING"),
            bigquery.SchemaField("exchange",     "STRING"),
            bigquery.SchemaField("start_date",   "STRING"),
            bigquery.SchemaField("end_date",     "STRING"),
            bigquery.SchemaField("fetched_at",   "TIMESTAMP"),
        ],
    )

    df["fetched_at"] = pd.to_datetime(df["fetched_at"])
    job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"  Loaded {len(df)} metadata rows into {table_ref}")


def main():
    print(f"\nBruin asset: raw_stock_metadata\n")
    
    try:
        rows = []
        for ticker in TICKERS:
            try:
                row = fetch_metadata(ticker)
                rows.append(row)
                time.sleep(0.5)
            except Exception as e:
                print(f"  ERROR processing {ticker}: {e}", file=sys.stderr)
                # Add a minimal row to keep the pipeline running
                rows.append({"ticker": ticker, "fetched_at": datetime.now().isoformat()})
                continue

        if not rows:
            print("No metadata fetched. Exiting.")
            return

        df = pd.DataFrame(rows)
        load_to_bigquery(df)
        print(f"\nDone — raw_stock_metadata complete.")
        print(df[["ticker", "company_name", "exchange"]].to_string(index=False))
    except Exception as e:
        print(f"\nFATAL ERROR in raw_stock_metadata: {e}", file=sys.stderr)
        raise

if __name__ == "__main__":
    main()
