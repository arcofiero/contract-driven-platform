{{
    config(
        materialized='table',
        file_format='delta',
        schema='gold'
    )
}}

SELECT
    event_date,
    payment_method,
    COUNT(*)                                                            AS total_payments,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END)                AS successful_payments,
    SUM(CASE WHEN status != 'success' THEN 1 ELSE 0 END)              AS failed_payments,
    ROUND(
        100.0 * SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) / COUNT(*),
        2
    )                                                                   AS success_rate_pct,
    SUM(CASE WHEN status = 'success' THEN amount ELSE 0 END)          AS successful_amount,
    SUM(amount)                                                         AS total_amount,
    MAX(ingested_at)                                                    AS last_updated_at
FROM {{ ref('silver_payments') }}
GROUP BY event_date, payment_method
ORDER BY event_date DESC, success_rate_pct ASC
