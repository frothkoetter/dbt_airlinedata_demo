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

{{
    config(
        materialized='incremental'
    )
}}

with connecting_flights as (

SELECT * FROM
  {{ ref('stg_airlinedata__unique_tickets') }} a,
  {{ ref('stg_airlinedata__flights') }}  o,
  {{ ref('stg_airlinedata__flights') }}  d,
  {{ ref('stg_airlinedata__airports') }}   oa,
  {{ ref('stg_airlinedata__airports') }}   da
WHERE
   a.leg1flightnum = o.flightnum
   AND a.leg1uniquecarrier = o.uniquecarrier
   AND a.leg1origin = o.origin
   AND a.leg1dest = o.dest
   AND a.leg1month = o.month
   AND a.leg1dayofmonth = o.dayofmonth
   AND a.leg1dayofweek = o.`dayofweek`
   AND a.leg2flightnum = d.flightnum
   AND a.leg2uniquecarrier = d.uniquecarrier
   AND a.leg2origin = d.origin
   AND a.leg2dest = d.dest
   AND a.leg2month = d.month
   AND a.leg2dayofmonth = d.dayofmonth
   AND a.leg2dayofweek = d.`dayofweek`
   AND d.origin = oa.iata
   AND d.dest = da.iata
   AND oa.country <> da.country )
select
  *
from connecting_flights
