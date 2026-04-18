import os
import time
import pandas as pd
from datetime import datetime, date
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

BQ_PROJECT   = os.getenv("GCP_PROJECT_ID", "tech-stock-analytics")
ML_DATASET   = "ml_stocks"
MART_DATASET = "marts_stocks"
SOURCE_TABLE = f"{BQ_PROJECT}.marts_stocks.mart_daily_ohlcv"
TARGET_TABLE = f"{BQ_PROJECT}.marts_stocks.mart_predictions"

TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA",
    "META", "TSLA", "AMD", "INTC", "ORCL",
]

FORECAST_HORIZON  = 65    # ~1 quarter of trading days
CONFIDENCE_LEVEL  = 0.90  # 90% prediction interval

client = bigquery.Client(project=BQ_PROJECT)


# ── Step 1: Create ARIMA_PLUS model for a single ticker ──────────────────────

def create_model(ticker: str):
    model_ref = f"`{BQ_PROJECT}.{ML_DATASET}.model_{ticker}`"
    print(f"\n  Training model for {ticker}...")
    print(f"  This takes 2-4 minutes per ticker...")

    sql = f"""
    CREATE OR REPLACE MODEL {model_ref}
    OPTIONS(
      model_type                = 'ARIMA_PLUS',
      time_series_timestamp_col = 'date',
      time_series_data_col      = 'adj_close',
      time_series_id_col        = 'ticker',
      auto_arima                = TRUE,
      data_frequency            = 'DAILY',
      decompose_time_series     = TRUE,
      clean_spikes_and_dips     = TRUE,
      adjust_step_changes       = TRUE,
      holiday_region            = 'US'
    ) AS
    SELECT
      CAST(date AS TIMESTAMP) AS date,
      ticker,
      adj_close
    FROM `{BQ_PROJECT}.marts_stocks.mart_daily_ohlcv`
    WHERE
      ticker    = '{ticker}'
      AND adj_close IS NOT NULL
    ORDER BY date
    """

    job = client.query(sql)
    job.result()  # waits for completion
    print(f"  Model for {ticker} trained successfully")


# ── Step 2: Evaluate model quality ───────────────────────────────────────────

def evaluate_model(ticker: str) -> dict:
    model_ref = f"`{BQ_PROJECT}.{ML_DATASET}.model_{ticker}`"

    # ARIMA_PLUS (Univariate) returns AIC, Log Likelihood, and Variance
    # We use LIMIT 1 to get the best model found by Auto ARIMA
    sql = f"""
    SELECT
      log_likelihood,
      AIC,
      variance
    FROM ML.ARIMA_EVALUATE(MODEL {model_ref})
    LIMIT 1
    """

    try:
        results = client.query(sql).to_dataframe()
        if results.empty:
            return {}

        row = results.iloc[0]
        metrics = {
            "ticker": ticker,
            "aic":          round(float(row["AIC"]), 2),
            "log_likelihood": round(float(row["log_likelihood"]), 2),
            "variance":     round(float(row["variance"]), 4),
        }
        print(f"  {ticker} — AIC: {metrics['aic']}  LogLikelihood: {metrics['log_likelihood']}")
        return metrics
    except Exception as e:
        print(f"  Evaluation error for {ticker}: {e}")
        return {"ticker": ticker}


# ── Step 3: Generate forecast for a single ticker ────────────────────────────

def generate_forecast(ticker: str) -> pd.DataFrame:
    model_ref = f"`{BQ_PROJECT}.{ML_DATASET}.model_{ticker}`"
    print(f"  Generating {FORECAST_HORIZON}-day forecast for {ticker}...")

    sql = f"""
    SELECT
      '{ticker}'                              AS ticker,
      CAST(forecast_timestamp AS DATE)        AS forecast_date,
      ROUND(forecast_value, 4)                AS predicted_close,
      ROUND(prediction_interval_lower_bound, 4) AS prediction_interval_lower_value,
      ROUND(prediction_interval_upper_bound, 4) AS prediction_interval_upper_value,
      {CONFIDENCE_LEVEL}                      AS confidence_level,
      CURRENT_DATE()                          AS model_run_date
    FROM
      ML.FORECAST(
        MODEL {model_ref},
        STRUCT(
          {FORECAST_HORIZON} AS horizon,
          {CONFIDENCE_LEVEL} AS confidence_level
        )
      )
    ORDER BY forecast_date
    """

    df = client.query(sql).to_dataframe()
    print(f"  {ticker}: {len(df)} forecast rows generated")
    return df


# ── Step 4: Write all forecasts into mart_predictions ────────────────────────

def write_predictions(df: pd.DataFrame):
    print(f"\nWriting {len(df)} total forecast rows to {TARGET_TABLE}...")

    # Clear existing predictions and replace with fresh run
    truncate_sql = f"""
    DELETE FROM `{TARGET_TABLE}`
    WHERE model_run_date = CURRENT_DATE()
    """
    try:
        client.query(truncate_sql).result()
        print("  Cleared today's existing predictions")
    except Exception:
        print("  Table empty or first run — skipping clear")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("ticker",                           "STRING"),
            bigquery.SchemaField("forecast_date",                    "DATE"),
            bigquery.SchemaField("predicted_close",                  "FLOAT64"),
            bigquery.SchemaField("prediction_interval_lower_value",  "FLOAT64"),
            bigquery.SchemaField("prediction_interval_upper_value",  "FLOAT64"),
            bigquery.SchemaField("confidence_level",                 "FLOAT64"),
            bigquery.SchemaField("model_run_date",                   "DATE"),
        ],
    )

    df["forecast_date"]    = pd.to_datetime(df["forecast_date"]).dt.date
    df["model_run_date"]   = pd.to_datetime(df["model_run_date"]).dt.date

    job = client.load_table_from_dataframe(df, TARGET_TABLE, job_config=job_config)
    job.result()
    print(f"  Successfully written {len(df)} rows into mart_predictions")


# ── Step 5: Print forecast summary ───────────────────────────────────────────

def print_summary(all_forecasts: pd.DataFrame, all_metrics: list):
    print("\n" + "="*60)
    print("PHASE 4 COMPLETE — FORECAST SUMMARY")
    print("="*60)

    # Get last known price per ticker from mart
    last_price_sql = f"""
    SELECT ticker, adj_close AS last_close, date AS last_date
    FROM `{BQ_PROJECT}.marts_stocks.mart_daily_ohlcv`
    WHERE date = (
      SELECT MAX(date) FROM `{BQ_PROJECT}.marts_stocks.mart_daily_ohlcv`
    )
    ORDER BY ticker
    """
    try:
        last_prices = client.query(last_price_sql).to_dataframe()
        last_prices = last_prices.set_index("ticker")
    except Exception:
        last_prices = pd.DataFrame()

    print(f"\n{'Ticker':<8} {'Last Close':>12} {'Q-End Forecast':>16} {'Lower':>10} {'Upper':>10} {'Change%':>10}")
    print("-"*70)

    for ticker in TICKERS:
        ticker_fc = all_forecasts[all_forecasts["ticker"] == ticker]
        if ticker_fc.empty:
            continue

        last_row  = ticker_fc.iloc[-1]
        predicted = last_row["predicted_close"]
        lower     = last_row["prediction_interval_lower_value"]
        upper     = last_row["prediction_interval_upper_value"]

        if not last_prices.empty and ticker in last_prices.index:
            last_close = last_prices.loc[ticker, "last_close"]
            change_pct = ((predicted - last_close) / last_close) * 100
            print(f"{ticker:<8} {last_close:>12.2f} {predicted:>16.2f} {lower:>10.2f} {upper:>10.2f} {change_pct:>+9.2f}%")
        else:
            print(f"{ticker:<8} {'N/A':>12} {predicted:>16.2f} {lower:>10.2f} {upper:>10.2f}")

    if all_metrics:
        print("\nModel quality metrics (Lower AIC = Better Fit):")
        print(f"\n{'Ticker':<8} {'AIC':>12} {'LogLikelihood':>15} {'Variance':>12}")
        print("-" * 50)
        for m in all_metrics:
            if "aic" in m:
                print(f"{m['ticker']:<8} {m['aic']:>12.2f} {m['log_likelihood']:>15.2f} {m['variance']:>12.4f}")

    print(f"\nForecast horizon : {FORECAST_HORIZON} trading days (~1 quarter)")
    print(f"Confidence level : {int(CONFIDENCE_LEVEL*100)}%")
    print(f"Predictions table: {TARGET_TABLE}")
    print(f"Run date         : {date.today()}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("="*60)
    print("PHASE 4 — BQML ARIMA_PLUS STOCK PRICE FORECASTING")
    print("="*60)
    print(f"Tickers          : {TICKERS}")
    print(f"Forecast horizon : {FORECAST_HORIZON} days (~1 quarter)")
    print(f"Confidence level : {int(CONFIDENCE_LEVEL*100)}%")
    print(f"Source table     : {SOURCE_TABLE}")
    print(f"Target table     : {TARGET_TABLE}")

    all_forecasts = []
    all_metrics   = []

    for i, ticker in enumerate(TICKERS):
        print(f"\n[{i+1}/{len(TICKERS)}] Processing {ticker}...")

        # Train
        try:
            create_model(ticker)
        except Exception as e:
            print(f"  TRAINING FAILED for {ticker}: {e}")
            continue

        # Evaluate
        metrics = evaluate_model(ticker)
        if metrics:
            all_metrics.append(metrics)

        # Forecast
        try:
            forecast_df = generate_forecast(ticker)
            if not forecast_df.empty:
                all_forecasts.append(forecast_df)
        except Exception as e:
            print(f"  FORECAST FAILED for {ticker}: {e}")
            continue

        # Small pause between tickers
        if i < len(TICKERS) - 1:
            print(f"  Pausing before next ticker...")
            time.sleep(2)

    if not all_forecasts:
        print("\nNo forecasts generated. Check errors above.")
        return

    # Combine and write all forecasts
    combined = pd.concat(all_forecasts, ignore_index=True)
    write_predictions(combined)

    # Print summary table
    print_summary(combined, all_metrics)


if __name__ == "__main__":
    main()