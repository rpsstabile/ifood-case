SELECT
    pickup_hour,
    avg_passenger_count
FROM main.ifood.passengers_by_hour
WHERE pickup_year = 2023
  AND pickup_month = 5
ORDER BY pickup_hour;