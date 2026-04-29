{{
    config(
        materialized='table',
        file_format='delta',
        schema='silver'
    )
}}

SELECT
    event_id,
    user_id,
    session_id,
    CAST(event_ts / 1000 AS TIMESTAMP)     AS event_ts,
    event_date,
    LOWER(TRIM(page))                       AS page,
    LOWER(TRIM(action))                     AS action,
    UPPER(TRIM(country_code))               AS country_code,
    device_type,
    CAST(duration_ms AS INT)                AS duration_ms,
    _ingested_at,
    _source_topic,
    _kafka_partition,
    _kafka_offset,
    _schema_version
FROM {{ source('bronze', 'clickstream') }}
WHERE _is_valid = true
  AND event_id IS NOT NULL
  AND TRIM(event_id) != ''
  AND user_id IS NOT NULL
  AND TRIM(user_id) != ''
