CREATE OR REPLACE TABLE
  `weather-orchestration-lab.weather_pipeline.weather_city_summary`
AS
SELECT
  city,
  MIN(date) AS first_date,
  MAX(date) AS last_date,
  COUNT(*) AS days_observed,
  ROUND(AVG(temperature_max_c), 2) AS avg_max_temperature_c,
  ROUND(AVG(temperature_min_c), 2) AS avg_min_temperature_c,
  ROUND(AVG(temperature_range_c), 2) AS avg_temperature_range_c,
  ROUND(SUM(precipitation_mm), 2) AS total_precipitation_mm
FROM
  `weather-orchestration-lab.weather_pipeline.weather_daily_clean`
GROUP BY city;
