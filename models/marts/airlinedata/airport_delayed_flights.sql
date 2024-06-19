{#
#  pivot a resultset top deparing airlines by airports
#}

with delayed_airport_flights as (
  SELECT
    date_format( from_unixtime( ( unix_timestamp(concat( year,'-', month, '-', dayofmonth, ' ' ,
          substring(lpad(deptime,4,'0'),1,2),':', substring(lpad(deptime,4,'0'),3,2) ,':00' )))) , 'yyyy/MM/dd HH:00:00'),
     flights.dest as destination_airport,
     airports.city as destination_city,
      max(flights.temp) as dest_temp,
      max(flights.wind_speed) as dest_wind_speed,
      max(flights.pressure) as dest_pressure,
      sum(flights.prediction) AS predicted_delayed,
      sum(flights.prediction_delay) AS predicted_delay_min
  FROM
     airlinedata.flights_final flights,
     airlinedata.airports_orc airports
  WHERE
      flights.dest = airports.iata
  GROUP BY
     date_format( from_unixtime( ( unix_timestamp(concat( year,'-', month, '-', dayofmonth, ' ' ,
          substring(lpad(deptime,4,'0'),1,2),':', substring(lpad(deptime,4,'0'),3,2) ,':00' )))) , 'yyyy/MM/dd HH:00:00'),
     flights.dest ,
     airports.city
)
select * from delayed_airport_flights;
