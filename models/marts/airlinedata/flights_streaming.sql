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


{{
    config(
          materialized='incremental'
        , incremental_strategy='append'
        , post_hook=
              "insert overwrite table dbt_airlinedata.flights_streaming_offset
              select
                max( unix_timestamp(concat( year,'-', month, '-', dayofmonth, ' ' ,
                    substring(lpad(deptime,4,'0'),1,2),':', substring(lpad(deptime,4,'0'),3,2) ,':00' )))
              from
                {{ this }}"

    )
}}

with flights as(
  select
    cast(year as int) as year,
    cast(month as int) as month,
    cast(dayofmonth as int) as dayofmonth,
    cast(dayofweek as int) as dayofweek,
    cast(deptime as int) as deptime,
    cast(crsdeptime as int) as crsdeptime,
    cast(arrtime as int) as arrtime,
    cast(crsarrtime as int) as crsarrtime,
    uniquecarrier,
    cast(flightnum as int) as flightnum,
    tailnum,
    cast(actualelapsedtime as int) as actualelapsedtime,
    cast(crselapsedtime as int) as crselapsedtime,
    cast(airtime as int) as airtime,
    cast(arrdelay as int) as arrdelay,
    cast(depdelay as int) as depdelay,
    origin,
    dest,
    cast( distance as integer ) as distance,
    cast(taxiin as int) as taxiin,
    cast(taxiout as int) as taxiout,
    cast(cancelled as int) as cancelled,
    cancellationcode,
    diverted,
    cast(carrierdelay as int) as carrierdelay,
    cast(weatherdelay as int) as weatherdelay,
    cast(nasdelay as int) as nasdelay,
    cast(securitydelay as int) as securitydelay,
    cast(lateaircraftdelay as int) as lateaircraftdelay,
    origin_lon,
    origin_lat,
    cast( dest_lon as float) as dest_lon,
    cast(dest_lat as float) as dest_lat,
    cast( translate( substr(prediction, instr(prediction,'prediction=')+11,1 ),'}','') as float) as prediction,
    cast( translate( substr(prediction, instr(prediction,'proba=')+6,3 ),'}','') as float) as proba,
    case translate( substr(prediction, instr(prediction,'prediction=')+11,1 ),'}','')
      when 1 then trunc(rand() * 99 + 1) end as prediction_delay ,
    cast( translate( substr( weather_json, instr(weather_json,'temp=')+5,5 ),',','') as float) as  temp,
    cast( translate( substr( weather_json, instr(weather_json,'pressure=')+9,6 ),',','') as float) as  presssure,
    cast( translate( substr( weather_json, instr(weather_json,'humidity=')+9,2 ),',','') as float) as  humidity,
    cast( translate( substr( weather_json, instr(weather_json,'speed=')+6,5 ),',','') as float) as  wind_speed,
    cast( translate( substr( weather_json, instr(weather_json,'all=')+4,3 ),'}','') as float) as clouds
 from {{ source('raw_airlinedata','flights_streaming_ice') }}
)
select
*
from flights
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  join  ( select max(t) as t
     from
       dbt_airlinedata.flights_streaming_offset ) t_offset
  on t_offset.t <
   unix_timestamp(concat( year,'-', month, '-', dayofmonth, ' ' ,
      substring(lpad(deptime,4,'0'),1,2),':', substring(lpad(deptime,4,'0'),3,2) ,':00' ))


{% endif %}

#}
