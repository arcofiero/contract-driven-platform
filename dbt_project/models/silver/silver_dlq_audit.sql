{{
    config(
        materialized='table',
        file_format='delta',
        schema='silver'
    )
}}

SELECT
    error_id,
    source_topic,
    error_type,
    error_message,
    event_date,
    _kafka_partition,
    _kafka_offset,
    raw_payload,
    _ingested_at,
    _is_valid
FROM {{ source('bronze', 'dlq') }}
ORDER BY _ingested_at DESC
