{{ config(
    materialized='table',
    file_format='delta',
    schema='silver'
) }}

SELECT
    event_id,
    user_id,
    session_id,
    LOWER(TRIM(page))                            AS page,
    LOWER(TRIM(action))                          AS action,
    element,
    referrer,
    user_agent,
    UPPER(TRIM(country))                         AS country_code,
    TIMESTAMP(CAST(event_ts / 1000 AS BIGINT))   AS event_ts,
    event_date,
    _ingested_at                                 AS ingested_at,
    _kafka_partition                             AS kafka_partition,
    _kafka_offset                                AS kafka_offset,
    CURRENT_TIMESTAMP()                          AS dbt_updated_at
FROM delta.`{{ env_var('LOCAL_DELTA_PATH', '/tmp/contract-driven-platform') }}/delta/bronze/clickstream`
WHERE _is_valid = true
  AND event_id IS NOT NULL
  AND user_id  IS NOT NULL
