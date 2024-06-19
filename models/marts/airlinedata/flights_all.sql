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

with flights_streaming as(
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
  from {{ ref('flights_streaming') }}
), flights_history as (
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
from {{ ref('stg_airlinedata__flights') }} )
select
  *
from
 flights_streaming
union
select
 *
from
 flights_history h
