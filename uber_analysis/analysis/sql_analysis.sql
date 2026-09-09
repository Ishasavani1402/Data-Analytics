select * from clean_location;

-- check new column consistency
select * from clean_trip_detail where total_booking_amount <= 0 or
trip_duration_min <= 0;

-- A . core analysis
-- 1 . total trip and check if any duplicate found or not
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT tripid) AS distinct_trip_ids,
    COUNT(*) - COUNT(DISTINCT tripid) AS duplicate_trip_ids
FROM clean_trip_detail;

-- 2 . core KPI
select round(avg(trip_distance),2) as avg_distance , 
round(sum(total_booking_amount),2) as total_booking_revenue ,
round(avg(trip_duration_min),2) as avg_trip_duration_min , 
round(sum(fare_amount),2) as total_fare_amount , 
round(sum(surgefee),2) as total_surge_fee from clean_trip_detail;

-- 3 . sql analysis

-- 1 . vehical wise total_booking_revenue
select vehicle , count(*) as total_trip ,
round(sum(total_booking_amount),2) as total_booking_revenue , 
round(sum(total_booking_amount) * 100.0 / sum(sum(total_booking_amount)) over() , 2) as pct_of_total_revenue
from clean_trip_detail group by vehicle order by total_booking_revenue desc;

-- 2. vehical wise trip_duration min 
select vehicle , round(sum(trip_duration_min),2) as total_trip_duration_min
from clean_trip_detail group by vehicle order by total_trip_duration_min desc;

-- 3 . which vehical gave higest fare amount(which vehical costly)
select vehicle , round(sum(fare_amount),2) as total_fare_amount from 
clean_trip_detail group by vehicle order by total_fare_amount desc;

-- 4 . vehical and passanger count 
select vehicle , round(sum(passenger_count),2) as total_passaenger from 
clean_trip_detail group by vehicle order by total_passaenger desc;

-- 5. trip distance and avg time duration of trip
select case when trip_distance <=2 then 'very short (0-2)'
when trip_distance  <= 5 then 'short (3-5)'
when trip_distance <= 10 then 'medium (6-10)'
when trip_distance <= 20 then 'long (11 -20)'
when trip_distance <= 50 then 'very long (21-50)'
else 'extrem long' end as trip_distance_bucket , count(*) as total_trip , 
round(avg(trip_duration_min),2) as avg_trip_duration 
from clean_trip_detail group by trip_distance_bucket order by avg_trip_duration desc;

-- 6 . payment type overview
select payment_type , count(*) as total_trip ,
round(sum(total_booking_amount) , 2) as total_booking_amt , 
round(sum(fare_amount),2) as total_fare_amount from clean_trip_detail
group by payment_type;

-- 7 . day wise trip
select distinct(dayname(pickup_date)) as days , count(*) as total_trip from clean_trip_detail
group by days order by field(days , 'Sunday' , 'Monday' , 'Tuesday' , 'Wednesday' , 
'Thursday' , 'Friday' , 'Saturday'); 

-- 8 . hourly analysis
select pickup_hour , count(*) as total_trip , 
round(sum(total_booking_amount) , 2) as total_booking_amt 
from clean_trip_detail group by pickup_hour order by pickup_hour;

-- 9 . daily booking 
select pickup_date , count(*) as total_trip , 
round(sum(total_booking_amount),2) as total_revenue
from clean_trip_detail group by pickup_date order by pickup_date


