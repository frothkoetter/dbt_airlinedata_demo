WITH aggregated_data AS (
    SELECT
        year AS flight_year,
        -- Dimensions
        uniquecarrier AS airline_code,
        origin AS origin_airport_code,
        dest AS destination_airport_code,
        tailnum AS tail_number,
        month AS flight_month,
        -- Metrics
        COUNT(*) AS total_flights,
        SUM(CASE WHEN depdelay > 0 THEN 1 ELSE 0 END) AS delayed_flights,
        ROUND(SUM(CASE WHEN depdelay > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS delay_percentage,
        AVG(distance) AS avg_flight_distance,
        AVG(airtime) AS avg_flight_duration_minutes,
        SUM(cancelled) AS total_cancelled_flights,
        SUM(diverted) AS total_diverted_flights
    FROM {{ ref('stg_airlinedata__flights') }}
    GROUP BY
        year,
        uniquecarrier, 
        origin,
        dest,
        tailnum,
        month
)
SELECT *
FROM aggregated_data
ORDER BY flight_year, airline_code, origin_airport_code, destination_airport_code, flight_month;
