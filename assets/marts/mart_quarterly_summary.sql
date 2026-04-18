/* @bruin
name: marts_stocks.mart_quarterly_summary
connection: gcp-tech-stocks
type: bq.sql
file: mart_quarterly_summary.sql
description: "Quarterly summary statistics"

depends:
  - marts_stocks.mart_daily_ohlcv

materialization:
  type: table
  strategy: create+replace
  dataset: marts_stocks
@bruin */

/* mart_quarterly_summary
   Quarterly aggregates per ticker.
   Used for quarter-over-quarter comparison in Looker Studio.
   Depends on: marts_stocks.mart_daily_ohlcv
*/

WITH

quarterly AS (
    SELECT
        ticker,
        EXTRACT(YEAR    FROM date) AS year,
        EXTRACT(QUARTER FROM date) AS quarter,
        date,
        open,
        high,
        low,
        close,
        adj_close,
        volume,
        daily_return,

        -- First and last trading day of the quarter per ticker
        FIRST_VALUE(date) OVER (
            PARTITION BY ticker, EXTRACT(YEAR FROM date), EXTRACT(QUARTER FROM date)
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS quarter_start_date,

        LAST_VALUE(date) OVER (
            PARTITION BY ticker, EXTRACT(YEAR FROM date), EXTRACT(QUARTER FROM date)
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS quarter_end_date,

        FIRST_VALUE(adj_close) OVER (
            PARTITION BY ticker, EXTRACT(YEAR FROM date), EXTRACT(QUARTER FROM date)
            ORDER BY date
        ) AS quarter_open_price,

        LAST_VALUE(adj_close) OVER (
            PARTITION BY ticker, EXTRACT(YEAR FROM date), EXTRACT(QUARTER FROM date)
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS quarter_close_price

    FROM `tech-stock-analytics.marts_stocks.mart_daily_ohlcv`
),

aggregated AS (
    SELECT
        ticker,
        year,
        quarter,
        MIN(quarter_start_date)  AS quarter_start_date,
        MAX(quarter_end_date)    AS quarter_end_date,
        MIN(quarter_open_price)  AS open_price,
        MAX(quarter_close_price) AS close_price,
        MAX(high)                AS high_price,
        MIN(low)                 AS low_price,
        ROUND(AVG(volume), 0)    AS avg_volume,
        COUNT(*)                 AS trading_days,

        -- Quarterly return %
        ROUND(
            SAFE_DIVIDE(
                MAX(quarter_close_price) - MIN(quarter_open_price),
                MIN(quarter_open_price)
            ) * 100, 4
        ) AS quarterly_return_pct,

        -- Max drawdown within the quarter
        ROUND(
            SAFE_DIVIDE(
                MIN(low) - MAX(high),
                MAX(high)
            ) * 100, 4
        ) AS max_drawdown

    FROM quarterly
    GROUP BY ticker, year, quarter
)

SELECT *
FROM aggregated


