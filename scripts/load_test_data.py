#!/usr/bin/env python3
"""
Load test CSV data into BigQuery raw tables.
This enables end-to-end testing of the pipeline without requiring live API calls.

Usage:
    python3 scripts/load_test_data.py
"""

import os
import sys
from pathlib import Path
from datetime import datetime
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from dotenv import load_dotenv

load_dotenv()

BQ_PROJECT = os.getenv("GCP_PROJECT_ID", "tech-stock-analytics")
RAW_DATASET = "raw_stocks"

DATA_DIR = Path(__file__).parent.parent / "data" / "raw"

# CSV files to load
FILES = {
    "stock_prices_test.csv": {
        "table": "raw_stock_prices",
        "schema": [
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("open", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("high", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("low", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("close", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("adj_close", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("volume", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("ticker", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="NULLABLE"),
        ],
    },
    "stock_metadata_test.csv": {
        "table": "raw_stock_metadata",
        "schema": [
            bigquery.SchemaField("ticker", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("company_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("exchange", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("start_date", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("end_date", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("fetched_at", "TIMESTAMP", mode="NULLABLE"),
        ],
    },
}


def load_csv_to_bq(csv_file: str, table_name: str, schema: list):
    """Load a CSV file into a BigQuery table."""
    client = bigquery.Client(project=BQ_PROJECT)
    csv_path = DATA_DIR / csv_file

    if not csv_path.exists():
        print(f"❌ File not found: {csv_path}")
        return False

    table_id = f"{BQ_PROJECT}.{RAW_DATASET}.{table_name}"

    print(f"\n📥 Loading {csv_file} → {table_id}")

    # Configure load job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip header
        schema=schema,
        write_disposition="WRITE_TRUNCATE",  # Replace table
        allow_quoted_newlines=True,
    )

    try:
        # Load CSV from local file
        with open(csv_path, "rb") as f:
            load_job = client.load_table_from_file(
                f, table_id, job_config=job_config
            )
            load_job.result()  # Wait for job to complete

        rows_loaded = load_job.output_rows
        print(f"✅ Loaded {rows_loaded:,} rows into {table_name}")
        return True

    except Exception as e:
        print(f"❌ Error loading {csv_file}: {e}")
        return False


def verify_data(table_name: str):
    """Verify data was loaded correctly."""
    client = bigquery.Client(project=BQ_PROJECT)
    table_id = f"{BQ_PROJECT}.{RAW_DATASET}.{table_name}"

    try:
        sql = f"SELECT COUNT(*) as row_count FROM `{table_id}`"
        result = client.query(sql).result()
        row_count = list(result)[0]["row_count"]
        print(f"   Row count: {row_count:,}")
        return True
    except Exception as e:
        print(f"   Verification failed: {e}")
        return False


def main():
    print("\n" + "=" * 70)
    print("LOADING TEST DATA INTO BIGQUERY")
    print("=" * 70)
    print(f"Project: {BQ_PROJECT}")
    print(f"Dataset: {RAW_DATASET}")

    all_success = True

    for csv_file, config in FILES.items():
        success = load_csv_to_bq(
            csv_file, config["table"], config["schema"]
        )
        all_success = all_success and success

        if success:
            verify_data(config["table"])

    print("\n" + "=" * 70)
    if all_success:
        print("✅ All data loaded successfully!")
        print("\nNext steps:")
        print("  1. Run the Bruin pipeline:")
        print("     cd bruin-pipeline && bruin run . --no-cache")
        print("  2. Then run BQML forecasting:")
        print("     python3 scripts/run_bqml.py")
    else:
        print("❌ Some files failed to load. Check errors above.")
        sys.exit(1)
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
