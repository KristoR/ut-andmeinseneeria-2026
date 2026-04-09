"""
European Weather Pipeline — Airflow DAG

Orkestreerib:
1. Riikide metaandmete laadimise REST Countries API-st
2. Ilmaandmete laadimise Open-Meteo Archive API-st
3. dbt transformatsioonid (staging -> intermediate -> marts)

Ühenduseparameetrid analytics-db jaoks on määratud compose.yml keskkonnamuutujas
AIRFLOW_CONN_ANALYTICS_DB, mis registreeritakse Airflow ühendusena "analytics_db".
"""

import pendulum

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

# Pealinnad, mille ilmaandmeid pärime
# NB: see on väike staatiline nimekiri — sobib ülatasemel hoida
CITIES = [
    {"name": "Tallinn", "country_code": "EST", "lat": 59.44, "lon": 24.75},
    {"name": "Helsinki", "country_code": "FIN", "lat": 60.17, "lon": 24.94},
    {"name": "Riga", "country_code": "LVA", "lat": 56.95, "lon": 24.11},
    {"name": "Stockholm", "country_code": "SWE", "lat": 59.33, "lon": 18.07},
    {"name": "Warsaw", "country_code": "POL", "lat": 52.23, "lon": 21.01},
]


@dag(
    dag_id="weather_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 3, 25, tz="UTC"),
    catchup=False,
    tags=["weather", "praktikum"],
)
def weather_pipeline():

    @task
    def extract_countries(ds=None) -> str:
        """
        Pärib riikide metaandmed REST Countries API-st ja salvestab faili.
        Tagastab XCom-i kaudu ainult failitee (mitte andmed ise).
        Impordid on taski sees — ei käivitu DAG-i parsimise ajal.
        """
        import json
        import urllib.parse
        import urllib.request
        from datetime import date

        params = urllib.parse.urlencode({
            "codes": ",".join(c["country_code"] for c in CITIES),
            "fields": "cca3,name,capital,latlng,population,area,currencies",
        })
        url = f"https://restcountries.com/v3.1/alpha?{params}"

        req = urllib.request.Request(
            url, headers={"User-Agent": "Airflow-Praktikum/1.0"}
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())

        countries = []
        for item in data:
            currency_code = ""
            currency_name = ""
            if item.get("currencies"):
                first_key = next(iter(item["currencies"]))
                currency_code = first_key
                currency_name = item["currencies"][first_key].get("name", "")

            countries.append(
                {
                    "country_code": item["cca3"],
                    "country_name": item["name"]["common"],
                    "capital": item.get("capital", [None])[0],
                    "latitude": item["latlng"][0] if item.get("latlng") else None,
                    "longitude": item["latlng"][1] if item.get("latlng") else None,
                    "population": item.get("population"),
                    "area_km2": item.get("area"),
                    "currency_code": currency_code,
                    "currency_name": currency_name,
                }
            )

        path = f"/tmp/countries_{ds or date.today().isoformat()}.json"
        with open(path, "w") as f:
            json.dump(countries, f)
        print(f"Laeti {len(countries)} riiki, salvestatud: {path}")
        return path

    @task
    def load_staging_countries(file_path: str) -> int:
        """
        Loeb riikide andmed failist (XCom annab ainult tee) ja laeb staging tabelisse.
        UPSERT tagab idempotentsuse.
        """
        import json
        from contextlib import closing

        from airflow.providers.postgres.hooks.postgres import PostgresHook

        with open(file_path) as f:
            countries = json.load(f)

        hook = PostgresHook(postgres_conn_id="analytics_db")
        with closing(hook.get_conn()) as conn, conn, conn.cursor() as cur:
            for c in countries:
                cur.execute(
                    """
                    INSERT INTO staging.countries_raw
                        (country_code, country_name, capital, latitude, longitude,
                         population, area_km2, currency_code, currency_name, loaded_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (country_code) DO UPDATE SET
                        country_name = EXCLUDED.country_name,
                        capital = EXCLUDED.capital,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        population = EXCLUDED.population,
                        area_km2 = EXCLUDED.area_km2,
                        currency_code = EXCLUDED.currency_code,
                        currency_name = EXCLUDED.currency_name,
                        loaded_at = NOW()
                    """,
                    (
                        c["country_code"],
                        c["country_name"],
                        c["capital"],
                        c["latitude"],
                        c["longitude"],
                        c["population"],
                        c["area_km2"],
                        c["currency_code"],
                        c["currency_name"],
                    ),
                )

        print(f"Laaditud {len(countries)} riiki staging tabelisse")
        return len(countries)

    @task
    def extract_weather(ds=None) -> str:
        """
        Pärib ilmaandmed Open-Meteo Archive API-st ja salvestab faili.
        Tagastab XCom-i kaudu ainult failitee (mitte andmed ise).

        ds — Airflow annab automaatselt loogilise kuupäeva stringina
        (nt "2025-04-01"). See tagab, et iga DAG-i käivitus pärib
        täpselt oma perioodi andmed.
        """
        import json
        import urllib.parse
        import urllib.request
        from datetime import date

        query_date = ds if ds else date.today().isoformat()

        weather_rows = []
        for city in CITIES:
            params = urllib.parse.urlencode({
                "latitude": city["lat"],
                "longitude": city["lon"],
                "start_date": query_date,
                "end_date": query_date,
                "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max",
                "timezone": "Europe/Tallinn",
            })
            url = f"https://archive-api.open-meteo.com/v1/archive?{params}"
            req = urllib.request.Request(
                url, headers={"User-Agent": "Airflow-Praktikum/1.0"}
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())

            daily = data.get("daily", {})
            if daily and daily.get("time"):
                weather_rows.append(
                    {
                        "city_name": city["name"],
                        "country_code": city["country_code"],
                        "latitude": city["lat"],
                        "longitude": city["lon"],
                        "observation_date": daily["time"][0],
                        "temp_max_c": daily["temperature_2m_max"][0],
                        "temp_min_c": daily["temperature_2m_min"][0],
                        "precipitation_mm": daily["precipitation_sum"][0],
                        "wind_speed_max_kmh": daily["wind_speed_10m_max"][0],
                    }
                )

        path = f"/tmp/weather_{query_date}.json"
        with open(path, "w") as f:
            json.dump(weather_rows, f)
        print(f"Laeti ilmaandmed {len(weather_rows)} linnale ({query_date}), salvestatud: {path}")
        return path

    @task
    def load_staging_weather(file_path: str) -> int:
        """
        Loeb ilmaandmed failist (XCom annab ainult tee) ja laeb staging tabelisse.
        UPSERT (country_code + observation_date) tagab idempotentsuse.
        """
        import json
        from contextlib import closing

        from airflow.providers.postgres.hooks.postgres import PostgresHook

        with open(file_path) as f:
            weather_rows = json.load(f)

        hook = PostgresHook(postgres_conn_id="analytics_db")
        with closing(hook.get_conn()) as conn, conn, conn.cursor() as cur:
            for w in weather_rows:
                cur.execute(
                    """
                    INSERT INTO staging.weather_raw
                        (city_name, country_code, latitude, longitude,
                         observation_date, temp_max_c, temp_min_c,
                         precipitation_mm, wind_speed_max_kmh, loaded_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (country_code, observation_date) DO UPDATE SET
                        city_name = EXCLUDED.city_name,
                        temp_max_c = EXCLUDED.temp_max_c,
                        temp_min_c = EXCLUDED.temp_min_c,
                        precipitation_mm = EXCLUDED.precipitation_mm,
                        wind_speed_max_kmh = EXCLUDED.wind_speed_max_kmh,
                        loaded_at = NOW()
                    """,
                    (
                        w["city_name"],
                        w["country_code"],
                        w["latitude"],
                        w["longitude"],
                        w["observation_date"],
                        w["temp_max_c"],
                        w["temp_min_c"],
                        w["precipitation_mm"],
                        w["wind_speed_max_kmh"],
                    ),
                )

        print(f"Laaditud {len(weather_rows)} ilmarida staging tabelisse")
        return len(weather_rows)

    @task.virtualenv(requirements=["duckdb==1.3.0"])
    def validate_with_duckdb() -> dict:
        """
        Andmekvaliteedi kontroll DuckDB-ga.
        DuckDB pole Airflow baasimage'is — @task.virtualenv installib selle.
        """
        import os

        import duckdb

        host = os.environ.get("POSTGRES_HOST", "analytics-db")
        port = os.environ.get("POSTGRES_PORT", "5432")
        user = os.environ.get("POSTGRES_USER", "praktikum")
        password = os.environ.get("POSTGRES_PASSWORD", "praktikum")
        dbname = os.environ.get("POSTGRES_DB", "praktikum")

        conn = duckdb.connect()
        conn.execute("INSTALL postgres; LOAD postgres;")
        conn.execute(
            f"""
            ATTACH 'host={host} port={port} dbname={dbname} user={user} password={password}'
            AS pg (TYPE postgres, READ_ONLY)
            """
        )

        weather_count = conn.execute(
            "SELECT COUNT(*) FROM pg.staging.weather_raw"
        ).fetchone()[0]

        countries_count = conn.execute(
            "SELECT COUNT(*) FROM pg.staging.countries_raw"
        ).fetchone()[0]

        null_temps = conn.execute(
            "SELECT COUNT(*) FROM pg.staging.weather_raw WHERE temp_max_c IS NULL"
        ).fetchone()[0]

        result = {
            "weather_rows": weather_count,
            "countries_rows": countries_count,
            "null_temperatures": null_temps,
            "status": "OK" if null_temps == 0 else "WARNING",
        }
        print(f"Valideerimise tulemus: {result}")
        conn.close()
        return result

    # -----------------------------------------------------------------------
    # dbt käivitamine BashOperator-iga
    # dbt-postgres on Airflow konteinerisse paigaldatud ja dbt_project
    # on volume-iga ühendatud /opt/airflow/dbt_project kausta.
    # -----------------------------------------------------------------------

    dbt_run = BashOperator(
        task_id="run_dbt",
        bash_command=(
            "dbt run"
            " --project-dir /opt/airflow/dbt_project"
            " --profiles-dir /opt/airflow/dbt_project"
        ),
    )

    dbt_test = BashOperator(
        task_id="test_dbt",
        bash_command=(
            "dbt test"
            " --project-dir /opt/airflow/dbt_project"
            " --profiles-dir /opt/airflow/dbt_project"
        ),
    )

    # -----------------------------------------------------------------------
    # Sõltuvuste defineerimine
    # -----------------------------------------------------------------------

    # Extract ja load
    countries_data = extract_countries()
    countries_loaded = load_staging_countries(countries_data)

    weather_data = extract_weather()
    weather_loaded = load_staging_weather(weather_data)

    # Paralleelsusmuster: dbt käivitub alles siis, kui mõlemad laadimised on valmis
    [countries_loaded, weather_loaded] >> dbt_run >> dbt_test

    # Valideerimine (virtualenv) jookseb pärast dbt teste
    dbt_test >> validate_with_duckdb()


weather_pipeline()
