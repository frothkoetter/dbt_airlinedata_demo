{#
# /*
#  * Copyright 2022 Cloudera, Inc.
#  *
#  * Licensed under the Apache License, Version 2.0 (the "License");
#  * you may not use this file except in compliance with the License.
#  * You may obtain a copy of the License at
#  *
#  *   http://www.apache.org/licenses/LICENSE-2.0
#  *
#  * Unless required by applicable law or agreed to in writing, software
#  * distributed under the License is distributed on an "AS IS" BASIS,
#  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  * See the License for the specific language governing permissions and
#  * limitations under the License.
#  */
#}


with flights as(
  select
  month,
  dayofmonth,
  dayofweek,
  deptime,
  crsdeptime,
  arrtime,
  crsarrtime,
  uniquecarrier,
  flightnum,
  tailnum,
  actualelapsedtime,
  crselapsedtime,
  airtime,
  arrdelay,
  depdelay,
  origin,
  dest,
  distance,
  taxiin,
  taxiout,
  cancelled,
  cancellationcode,
  diverted,
  carrierdelay,
  weatherdelay,
  nasdelay,
  securitydelay,
  lateaircraftdelay,
  year
  from {{ ref('stg_airlinedata__flights') }}
),
airlines as(
  select
    code,
    description
  from {{ ref('stg_airlinedata__airlines') }}
), delayed_flights as(
  SELECT
    SUM(flights.cancelled) AS num_flights_cancelled,
    SUM(1) AS total_num_flights,
    MIN(airlines.description) AS airline_name,
    airlines.code AS airline_code
  FROM
    flights
    JOIN airlines ON (flights.uniquecarrier = airlines.code)
  GROUP BY airlines.code
)
select
  delayed_flights.num_flights_cancelled,
  delayed_flights.total_num_flights,
  delayed_flights.airline_name,
  delayed_flights.airline_code
from delayed_flights
