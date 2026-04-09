-- Puhastatud riikide andmed: standardiseeritud nimed
-- Materialiseerimine: view

SELECT
    country_code,
    INITCAP(TRIM(country_name))  AS country_name,
    INITCAP(TRIM(capital))       AS capital,
    latitude,
    longitude,
    population,
    area_km2,
    UPPER(TRIM(currency_code))   AS currency_code,
    TRIM(currency_name)          AS currency_name,
    loaded_at
FROM {{ source('staging', 'countries_raw') }}
WHERE country_code IS NOT NULL
