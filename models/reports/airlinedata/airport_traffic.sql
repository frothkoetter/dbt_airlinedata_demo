WITH traffic_data AS (
    SELECT
        origin AS airport_code,
        COUNT(*) AS departures,
        0 as arrivals
    FROM {{ ref('stg_airlinedata__flights') }}
    GROUP BY origin
    UNION ALL
    SELECT
        dest AS airport_code,
        0 as departures,
        COUNT(*) AS arrivals
    FROM {{ ref('stg_airlinedata__flights') }}
    GROUP BY dest
)
SELECT
    airport_code,
    SUM(departures) AS total_departures,
    SUM(arrivals) AS total_arrivals
FROM traffic_data
GROUP BY airport_code
