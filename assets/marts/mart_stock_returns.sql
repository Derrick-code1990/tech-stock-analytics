/* @bruin
name: marts_stocks.mart_stock_returns
connection: gcp-tech-stocks
type: bq.sql
file: mart_stock_returns.sql
description: "Calculate stock returns"

depends:
  - marts_stocks.mart_daily_ohlcv

materialization:
  type: table
  strategy: create+replace
  dataset: marts_stocks
@bruin */

/* mart_stock_returns
   Daily, weekly, monthly and cumulative return metrics per ticker.
   Feeds the returns analysis dashboard in Looker Studio.
   Depends on: marts_stocks.mart_daily_ohlcv
*/

WITH

base AS (
    SELECT
        date,
        ticker,
        adj_close,
        daily_return,
        volume
    FROM `tech-stock-analytics.marts_stocks.mart_daily_ohlcv`
    WHERE adj_close IS NOT NULL
),

returns AS (
    SELECT
        date,
        ticker,
        adj_close,
        daily_return,
        volume,

        -- Weekly return (5 trading days)
        ROUND(
            SAFE_DIVIDE(
                adj_close - LAG(adj_close, 5) OVER (PARTITION BY ticker ORDER BY date),
                LAG(adj_close, 5) OVER (PARTITION BY ticker ORDER BY date)
            ), 6
        ) AS weekly_return,

        -- Monthly return (~21 trading days)
        ROUND(
            SAFE_DIVIDE(
                adj_close - LAG(adj_close, 21) OVER (PARTITION BY ticker ORDER BY date),
                LAG(adj_close, 21) OVER (PARTITION BY ticker ORDER BY date)
            ), 6
        ) AS monthly_return,

        -- Quarterly return (~63 trading days)
        ROUND(
            SAFE_DIVIDE(
                adj_close - LAG(adj_close, 63) OVER (PARTITION BY ticker ORDER BY date),
                LAG(adj_close, 63) OVER (PARTITION BY ticker ORDER BY date)
            ), 6
        ) AS quarterly_return,

        -- Year-to-date return
        ROUND(
            SAFE_DIVIDE(
                adj_close - FIRST_VALUE(adj_close) OVER (
                    PARTITION BY ticker, EXTRACT(YEAR FROM date)
                    ORDER BY date
                ),
                FIRST_VALUE(adj_close) OVER (
                    PARTITION BY ticker, EXTRACT(YEAR FROM date)
                    ORDER BY date
                )
            ), 6
        ) AS ytd_return,

        -- Cumulative return from start date
        ROUND(
            SAFE_DIVIDE(
                adj_close - FIRST_VALUE(adj_close) OVER (
                    PARTITION BY ticker ORDER BY date
                ),
                FIRST_VALUE(adj_close) OVER (
                    PARTITION BY ticker ORDER BY date
                )
            ), 6
        ) AS cumulative_return,

        -- Rolling 30-day Sharpe-like ratio (return / volatility)
        ROUND(
            SAFE_DIVIDE(
                AVG(daily_return) OVER (
                    PARTITION BY ticker ORDER BY date
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ),
                NULLIF(STDDEV(daily_return) OVER (
                    PARTITION BY ticker ORDER BY date
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ), 0)
            ), 4
        ) AS sharpe_ratio_30d,

        -- Up day flag
        CASE WHEN daily_return > 0 THEN 1 ELSE 0 END AS is_up_day,

        -- Win rate over last 30 days
        ROUND(
            AVG(CASE WHEN daily_return > 0 THEN 1.0 ELSE 0.0 END) OVER (
                PARTITION BY ticker ORDER BY date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ), 4
        ) AS win_rate_30d

    FROM base
)

SELECT *
FROM returns
ORDER BY ticker, date


