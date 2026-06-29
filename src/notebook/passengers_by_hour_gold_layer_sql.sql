-- Databricks notebook source
CREATE TABLE IF NOT EXISTS main.ifood.passengers_by_hour
(
    pickup_year INT,
    pickup_month INT,
    pickup_hour INT,
    total_trips BIGINT,
    total_passengers BIGINT,
    avg_passenger_count DOUBLE,
    processed_timestamp TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

MERGE INTO main.ifood.passengers_by_hour target

USING (

SELECT

    pickup_year,

    pickup_month,

    pickup_hour,

    COUNT(*) total_trips,

    SUM(passenger_count) total_passengers,

    ROUND(AVG(passenger_count),2) avg_passenger_count,

    current_timestamp() processed_timestamp

FROM (

    SELECT

        pickup_year,

        pickup_month,

        pickup_hour,

        passenger_count

    FROM main.ifood.yellow_taxi_silver

    UNION ALL

    SELECT

        pickup_year,

        pickup_month,

        pickup_hour,

        passenger_count

    FROM main.ifood.green_taxi_silver

)

GROUP BY

pickup_year,

pickup_month,

pickup_hour

) source

ON

target.pickup_year = source.pickup_year

AND target.pickup_month = source.pickup_month

AND target.pickup_hour = source.pickup_hour

WHEN NOT MATCHED THEN

INSERT *;