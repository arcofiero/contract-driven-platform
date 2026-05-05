{{ config(
    materialized='table',
    file_format='delta',
    schema='silver'
) }}

SELECT
    INITCAP(TRIM(city))                          AS city,
    UPPER(TRIM(country))                         AS country_code,
    CAST(temperature AS DOUBLE)                  AS temperature_c,
    CAST(feels_like  AS DOUBLE)                  AS feels_like_c,
    CAST(humidity    AS INT)                     AS humidity_pct,
    CAST(pressure    AS INT)                     AS pressure_hpa,
    CAST(wind_speed  AS DOUBLE)                  AS wind_speed_ms,
    LOWER(TRIM(description))                     AS weather_description,
    TIMESTAMP(CAST(event_ts / 1000 AS BIGINT))   AS event_ts,
    event_date,
    _ingested_at                                 AS ingested_at,
    _kafka_partition                             AS kafka_partition,
    _kafka_offset                                AS kafka_offset,
    CURRENT_TIMESTAMP()                          AS dbt_updated_at
FROM delta.`{{ env_var('LOCAL_DELTA_PATH', '/tmp/contract-driven-platform') }}/delta/bronze/weather`
WHERE _is_valid = true
  AND city IS NOT NULL
  AND temperature IS NOT NULL
  AND temperature BETWEEN -89 AND 60
  AND humidity IS NOT NULL
  AND humidity BETWEEN 0 AND 100
