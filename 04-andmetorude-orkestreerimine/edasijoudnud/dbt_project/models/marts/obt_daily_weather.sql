-- OBT (One Big Table): päevane ilm + riigi info ühes laias tabelis
-- Üks rida = üks linn + üks kuupäev
-- Materialiseerimine: table (JOIN + äriloogika)

SELECT
    w.observation_date,
    w.city_name,
    w.country_code,
    c.country_name,
    c.capital,
    c.population,
    c.currency_code,
    w.temp_max_c,
    w.temp_min_c,
    ROUND((w.temp_max_c + w.temp_min_c) / 2, 1) AS temp_avg_c,
    w.precipitation_mm,
    w.wind_speed_max_kmh,
    w.loaded_at
FROM {{ ref('int_weather') }} w
LEFT JOIN {{ ref('int_countries') }} c
    ON w.country_code = c.country_code
