
-- models/plane_statistics.sql

WITH flight_data AS (
    SELECT
        tailnum,
        COUNT(*) AS total_flights,
        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS total_cancelled,
        SUM(CASE WHEN diverted = '1' THEN 1 ELSE 0 END) AS total_diverted,
        SUM(nvl(distance,0)) as total_distance
    FROM {{ ref('stg_airlinedata__flights') }}
    GROUP BY tailnum
),

plane_data AS (
    SELECT
        tailnum,
        manufacturer,
        model,
        year,
        status,
        engine_type,
        aircraft_type
    FROM {{ ref('stg_airlinedata__planes') }}
)

SELECT
    fd.tailnum,
    pd.manufacturer,
    pd.model,
    pd.year,
    pd.status,
    pd.engine_type,
    pd.aircraft_type,
    fd.total_flights,
    fd.total_cancelled,
    fd.total_diverted,
    fd.total_distance
FROM flight_data fd
LEFT JOIN plane_data pd
ON fd.tailnum = pd.tailnum
ORDER BY fd.total_flights DESC;
