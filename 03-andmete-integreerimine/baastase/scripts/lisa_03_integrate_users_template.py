"""Lisaülesande näide kolme allika ETL töövoost.

Selles variandis maanduvad kõik allikad esmalt staging kihti.
Puhastus ja ühildamine toimub intermediate vaates.
Lõpptulemus laetakse analytics kihti.
"""

import json
import os

import psycopg2
import requests


API_URL = "https://jsonplaceholder.typicode.com/users"
PREFERENCES_PATH = "/data/teavituseelistused.json"


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "db"),
        port=os.environ.get("DB_PORT", "5432"),
        user=os.environ.get("DB_USER", "praktikum"),
        password=os.environ.get("DB_PASSWORD", "praktikum"),
        dbname=os.environ.get("DB_NAME", "praktikum"),
    )

def normalize_email(email):
    """Transform: muuda e-post ühendamiseks sobivaks."""
    if email is None:
        return None
    return email.strip().lower()

def fetch_api_users():
    """Andmete vastuvõtt: too kasutajad API-st ja jäta alles vajalikud väljad.

    Tagasta loend sõnastikest kujul:
    {
        "user_id": ...,
        "full_name": ...,
        "username": ...,
        "email": ...,
        "city": ...,
        "company_name": ...
    }

    TODO:
    1. Tee requests.get(API_URL, timeout=30).
    2. Kutsu välja response.raise_for_status().
    3. Võta response.json() tulemus muutujasse data.
    4. Tee list kasutajatest sobival kujul.
    """
    raise NotImplementedError("Tee fetch_api_users funktsioon valmis.")


def read_notification_preferences():
    """Andmete vastuvõtt: loe JSON failist teavituseelistused.

    Tagasta loend sõnastikest kujul:
    {
        "email": ...,
        "newsletter_opt_in": ...,
        "preferred_channel": ...,
        "updated_at": ...
    }

    TODO:
    1. Ava PREFERENCES_PATH fail.
    2. Loe fail json.load abil sisse.
    3. Tagasta loetud list.
    """
    raise NotImplementedError("Tee read_notification_preferences funktsioon valmis.")



def load_api_users(conn, api_users):
    """Laadimine staging kihti: salvesta API kasutajad tabelisse staging.api_users."""

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE staging.api_users;")
        for user in api_users:
            cur.execute(
                """
                INSERT INTO staging.api_users (
                    user_id,
                    full_name,
                    username,
                    email,
                    city,
                    company_name,
                    loaded_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, NOW());
                """,
                (
                    user["user_id"],
                    user["full_name"],
                    user["username"],
                    user["email"],
                    user["city"],
                    user["company_name"],
                ),
            )
    conn.commit()


def load_notification_preferences(conn, preferences):
    """Laadimine staging kihti: salvesta JSON andmed tabelisse staging.notification_preferences."""

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE staging.notification_preferences;")
        for item in preferences:
            cur.execute(
                """
                INSERT INTO staging.notification_preferences (
                    email,
                    newsletter_opt_in,
                    preferred_channel,
                    updated_at,
                    loaded_at
                )
                VALUES (%s, %s, %s, %s, NOW());
                """,
                (
                    item["email"],
                    item["newsletter_opt_in"],
                    item["preferred_channel"],
                    item["updated_at"],
                ),
            )
    conn.commit()


def count_status_rows(conn):
    """Tagasta, mitu staatusekirjet on staging.user_status tabelis."""

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM staging.user_status;")
        return cur.fetchone()[0]


def count_intermediate_rows(conn):
    """Tagasta, mitu rida annab intermediate.user_profile_enriched vaade."""

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM intermediate.user_profile_enriched;")
        return cur.fetchone()[0]


def load_final_rows_from_intermediate(conn):
    """Laadimine analytics kihti: lae lõpptabel intermediate vaatest.

    """Transform: ehita lõpptabeli read.
    TODO:
    1. Tühjenda analytics.user_profile.
    2. Lae andmed vaatest intermediate.user_profile_enriched.
    3. Salvesta lõpptabelisse väljad:
       user_id, full_name, username, email, city, company_name,
       account_status, source_system, newsletter_opt_in, preferred_channel.
    4. Tagasta sisestatud ridade arv.
    """
    raise NotImplementedError("Tee load_final_rows_from_intermediate valmis.")


def main():
    conn = get_connection()
    try:
        print("ETL etapp 1/3: Andmete vastuvõtt ja laadimine staging kihti")
        api_users = fetch_api_users()
        preferences = read_notification_preferences()
        print(f"- API-st tuli {len(api_users)} kasutajat.")
        print(f"- JSON failist tuli {len(preferences)} teavituseelistust.")
        load_api_users(conn, api_users)
        print(f"- Laadisin staging.api_users tabelisse {len(api_users)} rida.")
        load_notification_preferences(conn, preferences)
        print(
            "- Laadisin staging.notification_preferences tabelisse "
            f"{len(preferences)} rida."
        )
        print(f"- staging.user_status tabelis on {count_status_rows(conn)} staatusekirjet.")

        print("ETL etapp 2/3: Töötlus")
        intermediate_rows = count_intermediate_rows(conn)
        print(
            "- Intermediate vaade puhastas e-posti ja ühendas andmed "
            f"{intermediate_rows} kasutaja jaoks."
        )

        print("ETL etapp 3/3: Laadimine analytics kihti")
        inserted_rows = load_final_rows_from_intermediate(conn)
        print(
            f"- Laadisin analytics.user_profile tabelisse {inserted_rows} rida."
        )
        print("Valmis.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
