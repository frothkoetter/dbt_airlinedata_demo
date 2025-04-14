SELECT
    f.uniquecarrier AS airline_code,
    a.description as airline_name,
    COUNT(*) AS total_flights,
    AVG(f.depdelay) AS avg_departure_delay,
    AVG(f.arrdelay) AS avg_arrival_delay,
    SUM(f.cancelled) AS total_cancelled,
    current_date() as update_date
FROM {{ ref('stg_airlinedata__flights') }} f
LEFT JOIN {{ ref('stg_airlinedata__airlines') }} a
    ON f.uniquecarrier = a.code
GROUP BY  f.uniquecarrier, a.description
