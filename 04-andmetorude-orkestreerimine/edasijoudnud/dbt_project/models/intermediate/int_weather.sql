-- Puhastatud ilmaandmed: tüüpide kontroll, NULL-ide käsitlus
-- Materialiseerimine: view (arvutatakse iga päringu ajal)

SELECT
    city_name,
    country_code,
    latitude,
    longitude,
    observation_date,
    ROUND(temp_max_c, 1)        AS temp_max_c,
    ROUND(temp_min_c, 1)        AS temp_min_c,
    COALESCE(precipitation_mm, 0) AS precipitation_mm,
    COALESCE(wind_speed_max_kmh, 0) AS wind_speed_max_kmh,
    loaded_at
FROM {{ source('staging', 'weather_raw') }}
WHERE observation_date IS NOT NULL
  AND temp_max_c IS NOT NULL
