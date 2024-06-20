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

with traffic_cancel_airlines as (

SELECT
airlines.code           AS airline_code,
airlines.description    AS airline_name,
flights.year            AS year,
flights.month           AS month,
COUNT(*)                as flights_count,
SUM(flights.cancelled)  AS cancelled,
SUM( nvl(depdelay,0) )  AS departure_delay_minutes,
SUM( case when nvl(depdelay,0) > 0 then 1 end) as departure_delay_count
FROM
  {{ ref('stg_airlinedata__flights') }}  flights,
   {{ ref('stg_airlinedata__airlines') }} airlines
WHERE
  flights.uniquecarrier = airlines.code
GROUP BY
  airlines.code,
  airlines.description,
  flights.year,
  flights.month
  )
select
  *
from traffic_cancel_airlines
