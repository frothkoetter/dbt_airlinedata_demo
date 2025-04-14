SELECT
    year,
    uniquecarrier,
    COUNT(*) AS total_flights,
    SUM(CASE WHEN depdelay <= 0 AND arrdelay <= 0 THEN 1 ELSE 0 END) AS on_time_flights,
    ROUND(SUM(CASE WHEN depdelay <= 0 AND arrdelay <= 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS on_time_percentage
FROM {{ ref('stg_airlinedata__flights') }}
GROUP BY year, uniquecarrier
