create database uber;
use uber;
-- table relationship

--                  location_table
--                 ┌───────────────┐
--                 │ location_id   │
--                 │ location      │
--                 │ city          │
--                 └───────┬───────┘
--                         │
--              ┌──────────┴──────────┐
--              │                     │
--              ▼                     ▼
--     pu_location_id         do_location_id
--              ┌───────────────────────┐
--              │     trip_details      │
--              └───────────────────────┘

create table location (
LocationID smallint primary key, 
Location varchar(50) , 
City varchar(50));

create table trip_details(
TripID INT primary key,
PickupTime DATETIME NOT NULL,
DropOffTime DATETIME NOT NULL,
passenger_count TINYINT UNSIGNED NOT NULL,
trip_distance DECIMAL(8,2) NOT NULL,
PULocationID SMALLINT NOT NULL,
DOLocationID SMALLINT NOT NULL,
fare_amount DECIMAL(10,2) NOT NULL,
SurgeFee DECIMAL(10,2) NOT NULL,
Vehicle VARCHAR(30) NOT NULL,
Payment_type VARCHAR(30) NOT NULL , 
constraint fk_pu_location  foreign key (PULocationID) references location(LocationID) , 
constraint fk_do_location foreign key (DOLocationID) references location(LocationID)
);

select * from clean_location;
-- next is create new column 
