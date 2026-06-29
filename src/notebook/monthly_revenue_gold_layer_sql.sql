-- Databricks notebook source
CREATE TABLE IF NOT EXISTS main.ifood.monthly_revenue
(
    pickup_year INT,
    pickup_month INT,
    total_trips BIGINT,
    avg_total_amount DOUBLE,
    sum_total_amount DOUBLE,
    processed_timestamp TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

MERGE INTO main.ifood.monthly_revenue AS target

USING (

SELECT

    pickup_year,

    pickup_month,

    COUNT(*) AS total_trips,

    ROUND(AVG(total_amount),2) AS avg_total_amount,

    ROUND(SUM(total_amount),2) AS sum_total_amount,

    current_timestamp() AS processed_timestamp

FROM main.ifood.yellow_taxi_silver

GROUP BY

    pickup_year,

    pickup_month

) source

ON

target.pickup_year = source.pickup_year

AND

target.pickup_month = source.pickup_month

WHEN NOT MATCHED THEN

INSERT *
;