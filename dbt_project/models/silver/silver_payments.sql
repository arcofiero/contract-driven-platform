{{ config(
    materialized='table',
    file_format='delta',
    schema='silver'
) }}

SELECT
    payment_id,
    order_id,
    customer_id,
    CAST(amount AS DOUBLE)                       AS amount,
    UPPER(TRIM(currency))                        AS currency_code,
    LOWER(TRIM(method))                          AS payment_method,
    LOWER(TRIM(status))                          AS status,
    LOWER(TRIM(provider))                        AS provider,
    UPPER(TRIM(region))                          AS region,
    TIMESTAMP(CAST(event_ts / 1000 AS BIGINT))   AS event_ts,
    event_date,
    _ingested_at                                 AS ingested_at,
    _kafka_partition                             AS kafka_partition,
    _kafka_offset                                AS kafka_offset,
    CURRENT_TIMESTAMP()                          AS dbt_updated_at
FROM delta.`{{ env_var('LOCAL_DELTA_PATH', '/tmp/contract-driven-platform') }}/delta/bronze/payments`
WHERE _is_valid = true
  AND amount    > 0
  AND payment_id IS NOT NULL
  AND order_id   IS NOT NULL
  AND method     IS NOT NULL
