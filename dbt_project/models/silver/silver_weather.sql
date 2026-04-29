{{
    config(
        materialized='table',
        file_format='delta',
        schema='silver'
    )
}}

SELECT
    city,
    UPPER(TRIM(country_code))               AS country_code,
    CAST(event_ts / 1000 AS TIMESTAMP)      AS event_ts,
    event_date,
    CAST(temperature_c AS DECIMAL(6,2))     AS temperature_c,
    CAST(humidity_pct AS DECIMAL(5,2))      AS humidity_pct,
    CAST(wind_speed_kmh AS DECIMAL(6,2))    AS wind_speed_kmh,
    LOWER(TRIM(condition))                  AS condition,
    _ingested_at,
    _source_topic,
    _kafka_partition,
    _kafka_offset,
    _schema_version
FROM {{ source('bronze', 'weather') }}
WHERE _is_valid = true
  AND city IS NOT NULL
  AND TRIM(city) != ''
  AND temperature_c BETWEEN -89 AND 57
  AND humidity_pct BETWEEN 0 AND 100
