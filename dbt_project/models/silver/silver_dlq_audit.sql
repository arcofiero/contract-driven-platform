{{ config(
    materialized='table',
    file_format='delta',
    schema='silver'
) }}

SELECT
    error_type,
    source_topic,
    error_message,
    raw_payload,
    event_date,
    _ingested_at                                 AS ingested_at,
    _kafka_partition                             AS kafka_partition,
    _kafka_offset                                AS kafka_offset,
    CURRENT_TIMESTAMP()                          AS dbt_updated_at
FROM delta.`{{ env_var('LOCAL_DELTA_PATH', '/tmp/contract-driven-platform') }}/delta/bronze/dlq`
