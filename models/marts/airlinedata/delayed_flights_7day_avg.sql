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



with avg_day as (
  select concat(year, lpad(month,2,'0'), lpad(dayofmonth,2,'0'))       as tag,
         sum(arrdelay)                 as val
  from
   {{ ref('stg_airlinedata__flights') }}
  where
   year = 1995
  group by
   concat(year, lpad(month,2,'0'),
                   lpad(dayofmonth,2,'0'))
)
select *,
round( avg(val) over (order by tag rows between 6 preceding and current row),0) as 7day_avg
from avg_day
