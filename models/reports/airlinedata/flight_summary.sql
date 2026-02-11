{{ config(
    materialized='table',
    file_format='PARQUET',
    table_format='ICEBERG'
) }}

SELECT
    f.flightnum,
    f.origin,
    oa.airport AS origin_airport_name,
    f.dest,
    da.airport AS destination_airport_name,
    f.uniquecarrier,
    a.description AS airline_name,
    f.depdelay,
    f.arrdelay,
    f.distance,
    CASE
        WHEN f.depdelay > 0 THEN 'Delayed'
        ELSE 'On-Time'
    END AS departure_status
FROM {{ ref('stg_airlinedata__flights') }} f
LEFT JOIN {{ ref('stg_airlinedata__airlines') }} a
    ON f.uniquecarrier = a.code
LEFT JOIN {{ ref('stg_airlinedata__airports') }} oa
    ON f.origin = oa.iata
LEFT JOIN {{ ref('stg_airlinedata__airports') }} da
    ON f.dest = da.iata
