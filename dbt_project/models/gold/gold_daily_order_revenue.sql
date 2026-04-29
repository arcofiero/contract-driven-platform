{{
    config(
        materialized='table',
        file_format='delta',
        schema='gold'
    )
}}

SELECT
    event_date,
    currency_code,
    COUNT(DISTINCT order_id)                AS order_count,
    SUM(quantity)                           AS total_units_sold,
    SUM(line_total)                         AS gross_revenue,
    AVG(unit_price)                         AS avg_unit_price,
    MAX(_ingested_at)                       AS last_updated_at
FROM {{ ref('silver_orders') }}
GROUP BY event_date, currency_code
ORDER BY event_date DESC, gross_revenue DESC
