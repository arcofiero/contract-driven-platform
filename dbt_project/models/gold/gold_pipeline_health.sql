{{
    config(
        materialized='table',
        file_format='delta',
        schema='gold'
    )
}}

SELECT
    event_date,
    source_topic,
    error_type,
    COUNT(*)                AS violation_count,
    MIN(_ingested_at)       AS first_seen_at,
    MAX(_ingested_at)       AS last_seen_at
FROM {{ ref('silver_dlq_audit') }}
GROUP BY event_date, source_topic, error_type
ORDER BY event_date DESC, violation_count DESC
