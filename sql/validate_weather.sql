ASSERT (
  SELECT COUNT(*) > 0
  FROM `weather-orchestration-lab.weather_pipeline.weather_daily_clean`
) AS 'The clean weather table is empty.';

ASSERT (
  SELECT COUNT(*) = 0
  FROM `weather-orchestration-lab.weather_pipeline.weather_daily_clean`
  WHERE city IS NULL
     OR TRIM(city) = ''
     OR date IS NULL
     OR temperature_max_c IS NULL
     OR temperature_min_c IS NULL
     OR precipitation_mm IS NULL
     OR temperature_range_c IS NULL
) AS 'Required weather fields contain null or empty values.';

ASSERT (
  SELECT COUNT(*) = 0
  FROM (
    SELECT city, date
    FROM `weather-orchestration-lab.weather_pipeline.weather_daily_clean`
    GROUP BY city, date
    HAVING COUNT(*) > 1
  )
) AS 'Duplicate city/date keys exist.';

ASSERT (
  SELECT COUNT(*) = 0
  FROM `weather-orchestration-lab.weather_pipeline.weather_daily_clean`
  WHERE temperature_max_c < temperature_min_c
     OR precipitation_mm < 0
     OR ABS(
       temperature_range_c
       - ROUND(temperature_max_c - temperature_min_c, 2)
     ) > 0.000001
) AS 'Weather measurements or calculated ranges are invalid.';

SELECT 'Weather validation passed' AS result;
