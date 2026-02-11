{{ config(
    materialized='table',
    file_format='PARQUET',
    table_format='ICEBERG'
) }}

SELECT
    origin AS origin_airport_code,
    dest AS destination_airport_code,
    COUNT(*) AS total_flights,
    SUM(CASE WHEN depdelay > 0 THEN 1 ELSE 0 END) AS delayed_flights,
    ROUND(SUM(CASE WHEN depdelay > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS delay_percentage
FROM {{ ref('stg_airlinedata__flights') }}
GROUP BY origin, dest
