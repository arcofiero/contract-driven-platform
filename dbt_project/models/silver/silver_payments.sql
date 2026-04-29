{{
    config(
        materialized='table',
        file_format='delta',
        schema='silver'
    )
}}

SELECT
    payment_id,
    order_id,
    customer_id,
    CAST(event_ts / 1000 AS TIMESTAMP)         AS event_ts,
    event_date,
    CAST(amount AS DECIMAL(18,2))               AS amount,
    UPPER(TRIM(currency_code))                  AS currency_code,
    LOWER(TRIM(payment_method))                 AS payment_method,
    LOWER(TRIM(status))                         AS status,
    CASE
        WHEN LOWER(TRIM(status)) = 'success' THEN true
        ELSE false
    END                                         AS is_successful,
    _ingested_at,
    _source_topic,
    _kafka_partition,
    _kafka_offset,
    _schema_version
FROM {{ source('bronze', 'payments') }}
WHERE _is_valid = true
  AND payment_id IS NOT NULL
  AND TRIM(payment_id) != ''
  AND amount > 0
