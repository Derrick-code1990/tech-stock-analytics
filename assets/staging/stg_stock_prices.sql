/* @bruin
name: staging_stocks.stg_stock_prices
type: bq.sql
connection: gcp-tech-stocks
description: "Stage raw stock prices for further processing"

depends:
  - raw_stock_prices

materialization:
  type: table
  strategy: create+replace
  dataset: staging_stocks
  partition_by: date
  cluster_by:
    - ticker
@bruin */

/* stg_stock_prices
   Cleans raw OHLCV data and adds daily return and price range.
   Depends on: raw_stocks.raw_stock_prices
*/

WITH

source AS (
    SELECT
        date,
        ticker,
        open,
        high,
        low,
        close,
        adj_close,
        volume,
        ingested_at,
        -- Row number to deduplicate — keep latest ingested record per date+ticker
        ROW_NUMBER() OVER (
            PARTITION BY date, ticker
            ORDER BY ingested_at DESC
        ) AS row_num
    FROM `tech-stock-analytics.raw_stocks.raw_stock_prices`
    WHERE
        date IS NOT NULL
        AND ticker IS NOT NULL
        AND close IS NOT NULL
        AND close > 0
        AND open > 0
        AND volume >= 0
),

deduplicated AS (
    SELECT * EXCEPT (row_num)
    FROM source
    WHERE row_num = 1
),

with_returns AS (
    SELECT
        date,
        ticker,
        open,
        high,
        low,
        close,
        adj_close,
        volume,
        ingested_at,

        -- Daily return based on adjusted close
        ROUND(
            SAFE_DIVIDE(
                adj_close - LAG(adj_close) OVER (PARTITION BY ticker ORDER BY date),
                LAG(adj_close) OVER (PARTITION BY ticker ORDER BY date)
            ), 6
        ) AS daily_return,

        -- Intraday price range
        ROUND(high - low, 4) AS price_range,

        -- Data quality flag
        CASE
            WHEN close IS NULL THEN FALSE
            WHEN close <= 0    THEN FALSE
            WHEN volume < 0    THEN FALSE
            WHEN high < low    THEN FALSE
            WHEN high < close  THEN FALSE
            WHEN low  > close  THEN FALSE
            ELSE TRUE
        END AS is_valid

    FROM deduplicated
)

SELECT *
FROM with_returns
