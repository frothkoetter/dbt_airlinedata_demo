{#
#  pivot a resultset top deparing airlines by airports
#}

with dep_flights as (
select
 origin       as airport,
 uniquecarrier as departing_airline,
 count(1)     as cnt_flights
 from
 {{ ref('stg_airlinedata__flights') }}
group by
origin,
uniquecarrier
), airlines_ranked as (
select
  airport,
  departing_airline,
      rank()
           over (PARTITION BY airport ORDER BY cnt_flights DESC) as rank
 from dep_flights
 )
select airport,
max( case when rank = 1 then departing_airline end ) as one,
max( case when rank = 2 then departing_airline end ) as two,
max(case when rank = 3 then departing_airline end) as three
from airlines_ranked
where rank <= 3
group by
 airport
