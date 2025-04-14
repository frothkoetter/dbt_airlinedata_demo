WITH route_details AS (
    SELECT
        f.origin AS origin_airport_code,
        oa.airport AS origin_airport_name,
        f.dest AS destination_airport_code,
        da.airport AS destination_airport_name,
        f.uniquecarrier AS airline_code,
        a.description AS airline_name,
        COUNT(*) AS total_flights,
        ROUND(AVG(f.distance), 2) AS avg_distance
    FROM {{ ref('stg_airlinedata__flights') }} f
    LEFT JOIN {{ ref('stg_airlinedata__airports') }} oa
        ON f.origin = oa.iata
    LEFT JOIN {{ ref('stg_airlinedata__airports') }} da
        ON f.dest = da.iata
    LEFT JOIN {{ ref('stg_airlinedata__airlines') }} a
        ON f.uniquecarrier = a.code
    GROUP BY f.origin, oa.airport, f.dest, da.airport, f.uniquecarrier, a.description
)
SELECT
    origin_airport_code,
    origin_airport_name,
    destination_airport_code,
    destination_airport_name,
    airline_code,
    airline_name,
    total_flights,
    avg_distance
FROM route_details
ORDER BY total_flights DESC
LIMIT 50
