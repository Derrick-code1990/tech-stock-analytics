/* @bruin
name: marts_stocks.mart_daily_ohlcv
connection: gcp-tech-stocks
type: bq.sql
file: mart_daily_ohlcv.sql
description: "Daily OHLCV summary for all stocks"

depends:
  - staging_stocks.stg_stock_prices

materialization:
  type: table
  strategy: create+replace
  dataset: marts_stocks
  partition_by: date
  cluster_by:
    - ticker
@bruin */

/* mart_daily_ohlcv
   Clean daily OHLCV enriched with moving averages and volatility.
   Primary table for Looker Studio historical dashboards.
   Depends on: staging_stocks.stg_stock_prices
*/

WITH

base AS (
    SELECT *
    FROM `tech-stock-analytics.staging_stocks.stg_stock_prices`
    WHERE is_valid = TRUE
),

enriched AS (
    SELECT
        date,
        ticker,
        open,
        high,
        low,
        close,
        adj_close,
        volume,
        daily_return,
        price_range,

        -- Moving averages on adjusted close
        ROUND(AVG(adj_close) OVER (
            PARTITION BY ticker
            ORDER BY date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 4) AS ma_7,

        ROUND(AVG(adj_close) OVER (
            PARTITION BY ticker
            ORDER BY date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ), 4) AS ma_30,

        ROUND(AVG(adj_close) OVER (
            PARTITION BY ticker
            ORDER BY date
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ), 4) AS ma_90,

        ROUND(AVG(adj_close) OVER (
            PARTITION BY ticker
            ORDER BY date
            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        ), 4) AS ma_200,

        -- 30-day rolling volatility (std dev of daily returns)
        ROUND(STDDEV(daily_return) OVER (
            PARTITION BY ticker
            ORDER BY date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ), 6) AS volatility_30,

        -- Volume moving average (30-day)
        ROUND(AVG(volume) OVER (
            PARTITION BY ticker
            ORDER BY date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ), 0) AS avg_volume_30,

        -- Price distance from 52-week high/low
        ROUND(MAX(high) OVER (
            PARTITION BY ticker
            ORDER BY date
            ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
        ), 4) AS week_52_high,

        ROUND(MIN(low) OVER (
            PARTITION BY ticker
            ORDER BY date
            ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
        ), 4) AS week_52_low,

        -- Cumulative return from 2015-01-01
        ROUND(
            SAFE_DIVIDE(
                adj_close - FIRST_VALUE(adj_close) OVER (
                    PARTITION BY ticker ORDER BY date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ),
                FIRST_VALUE(adj_close) OVER (
                    PARTITION BY ticker ORDER BY date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                )
            ), 6
        ) AS cumulative_return

    FROM base
)

SELECT *
FROM enriched



