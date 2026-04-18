/* @bruin
name: marts_stocks.bqml_create_models
connection: gcp-tech-stocks
type: bq.sql
file: bqml_create_models.sql
description: "Create BQML models for stock price prediction"

depends:
  - marts_stocks.mart_daily_ohlcv
@bruin */

-- AAPL
CREATE OR REPLACE MODEL `tech-stock-analytics.ml_stocks.model_AAPL`
OPTIONS(
  model_type             = 'ARIMA_PLUS',
  time_series_timestamp_col = 'date',
  time_series_data_col   = 'adj_close',
  time_series_id_col     = 'ticker',
  auto_arima             = TRUE,
  data_frequency         = 'DAILY',
  decompose_time_series  = TRUE,
  clean_spikes_and_dips  = TRUE,
  adjust_step_changes    = TRUE,
  holiday_region         = 'US'
) AS
SELECT
  CAST(date AS TIMESTAMP) AS date,
  ticker,
  adj_close
FROM `tech-stock-analytics.marts_stocks.mart_daily_ohlcv`
WHERE
  ticker    = 'AAPL'
  AND adj_close IS NOT NULL


