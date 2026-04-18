/* @bruin
name: staging_stocks.stg_stock_metadata
type: bq.sql
connection: gcp-tech-stocks
description: "Stage raw stock metadata for further processing"

depends:
  - raw_stock_metadata

materialization:
  type: table
  strategy: create+replace
  dataset: staging_stocks
@bruin */

/* stg_stock_metadata
   Cleans and standardises company metadata.
   Depends on: raw_stocks.raw_stock_metadata
*/

SELECT
    UPPER(TRIM(ticker))       AS ticker,
    TRIM(company_name)        AS company_name,
    TRIM(description)         AS description,
    UPPER(TRIM(exchange))     AS exchange,
    SAFE_CAST(start_date AS DATE) AS listing_start_date,
    CASE
        WHEN end_date IS NULL OR TRIM(end_date) = '' THEN NULL
        ELSE SAFE_CAST(end_date AS DATE)
    END                       AS listing_end_date,
    CASE
        WHEN end_date IS NULL OR TRIM(end_date) = '' THEN TRUE
        ELSE FALSE
    END                       AS is_active,
    fetched_at

FROM `tech-stock-analytics.raw_stocks.raw_stock_metadata`
WHERE
    ticker IS NOT NULL
    AND TRIM(ticker) != ''
