-- Staging skeema ja tabelid
-- Airflow DAG laeb API-dest andmeid siia. dbt loeb siit edasi.

CREATE SCHEMA IF NOT EXISTS staging;

-- Riikide toorandmed (REST Countries API)
CREATE TABLE IF NOT EXISTS staging.countries_raw (
    country_code    VARCHAR(3) PRIMARY KEY,
    country_name    VARCHAR(100) NOT NULL,
    capital         VARCHAR(100),
    latitude        NUMERIC(7,4),
    longitude       NUMERIC(7,4),
    population      BIGINT,
    area_km2        NUMERIC(12,2),
    currency_code   VARCHAR(3),
    currency_name   VARCHAR(50),
    loaded_at       TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Ilmaandmete toorandmed (Open-Meteo Archive API)
CREATE TABLE IF NOT EXISTS staging.weather_raw (
    city_name           VARCHAR(100) NOT NULL,
    country_code        VARCHAR(3) NOT NULL,
    latitude            NUMERIC(7,4),
    longitude           NUMERIC(7,4),
    observation_date    DATE NOT NULL,
    temp_max_c          NUMERIC(5,1),
    temp_min_c          NUMERIC(5,1),
    precipitation_mm    NUMERIC(7,1),
    wind_speed_max_kmh  NUMERIC(5,1),
    loaded_at           TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (country_code, observation_date)
);

-- ISS hetkeilma toorandmed (ISS API + Open-Meteo)
CREATE TABLE IF NOT EXISTS staging.iss_weather_raw (
    observation_time    TIMESTAMP PRIMARY KEY,
    iss_latitude        NUMERIC(8,4),
    iss_longitude       NUMERIC(8,4),
    temp_c              NUMERIC(5,1),
    wind_speed_kmh      NUMERIC(5,1),
    weather_code        INTEGER,
    loaded_at           TIMESTAMP NOT NULL DEFAULT NOW()
);
