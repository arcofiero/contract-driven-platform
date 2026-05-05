{{ config(
    materialized='table',
    file_format='delta',
    schema='silver'
) }}

WITH exploded AS (
    SELECT
        order_id,
        customer_id,
        CAST(event_ts / 1000 AS TIMESTAMP)      AS event_ts,
        event_date,
        UPPER(TRIM(currency))                    AS currency_code,
        status,
        shipping_address.city                    AS shipping_address_city,
        shipping_address.country                 AS shipping_address_country,
        item.product_id                          AS product_id,
        item.quantity                            AS quantity,
        CAST(item.unit_price   AS DECIMAL(18,2)) AS unit_price,
        CAST(item.quantity * item.unit_price AS DECIMAL(18,2)) AS line_total,
        _ingested_at,
        _source_topic,
        _kafka_partition,
        _kafka_offset,
        _schema_version,
        _is_valid
    FROM delta.`{{ env_var('LOCAL_DELTA_PATH', '/tmp/contract-driven-platform') }}/delta/bronze/orders`
    LATERAL VIEW EXPLODE(items) t AS item
    WHERE _is_valid = true
      AND order_id IS NOT NULL
      AND TRIM(order_id) != ''
)

SELECT
    order_id,
    customer_id,
    event_ts,
    event_date,
    CASE WHEN currency_code IN ('USD','EUR','GBP','INR','JPY')
         THEN currency_code ELSE 'UNKNOWN' END  AS currency_code,
    status,
    shipping_address_city,
    shipping_address_country,
    product_id,
    quantity,
    unit_price,
    line_total,
    _ingested_at,
    _source_topic,
    _kafka_partition,
    _kafka_offset,
    _schema_version
FROM exploded
WHERE quantity  >= 0
  AND unit_price >= 0
