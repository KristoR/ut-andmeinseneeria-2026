"""Kommenteeritud näide kahe allika ETL töövoost.

See skript jagab töövoo rollid kihtidesse:

- staging: allikalähedased andmed
- intermediate: puhastatud võtmed ja ühendatud vaated
- analytics: lõpptabel analüüsi jaoks
"""

import os

import psycopg2
import requests


API_URL = "https://jsonplaceholder.typicode.com/users"


def get_connection():
    """Loo andmebaasiühendus keskkonnamuutujate põhjal."""

    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "db"),
        port=os.environ.get("DB_PORT", "5432"),
        user=os.environ.get("DB_USER", "praktikum"),
        password=os.environ.get("DB_PASSWORD", "praktikum"),
        dbname=os.environ.get("DB_NAME", "praktikum"),
    )


def fetch_api_users():
    """Andmete vastuvõtt: loe kasutajad API-st."""

    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    data = response.json()

    users = []
    for item in data:
        users.append(
            {
                "user_id": item["id"],
                "full_name": " ".join(item["name"].split()),
                "username": item["username"],
                "email": item["email"],
                "city": item["address"]["city"],
                "company_name": item["company"]["name"],
            }
        )

    return users


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
    """Laadimine analytics kihti: lae lõpptabel `intermediate` vaatest.

def load_final_rows_from_intermediate(conn):
    """Laadimine analytics kihti: lae lõpptabel intermediate vaatest."""

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE analytics.user_profile;")
        cur.execute(
            """
            INSERT INTO analytics.user_profile (
                user_id,
                full_name,
                username,
                email,
                city,
                company_name,
                account_status,
                source_system,
                loaded_at
            )
            SELECT
                user_id,
                full_name,
                username,
                email,
                city,
                company_name,
                account_status,
                source_system,
                NOW()
            FROM intermediate.user_profile_enriched
            ORDER BY user_id;
            """
        )

        # `rowcount` ütleb, mitu rida eelmine SQL-käsk mõjutas.
        inserted_rows = cur.rowcount

    conn.commit()
    return inserted_rows


def main():
    """Käivita kogu töövoog õiges järjekorras.

    `main()` on selle faili põhitöövoog.
    Siin kutsume eelnevad funktsioonid ükshaaval välja ja prindime
    iga etapi järel lühikese vahekokkuvõtte.
    """

    # Siit algab kogu skripti peamine muutujate teekond:
    # get_connection() -> conn
    # fetch_api_users() -> api_users
    # count_intermediate_rows(conn) -> intermediate_rows
    # load_final_rows_from_intermediate(conn) -> inserted_rows
    conn = get_connection()

    # `try/finally` tähendab: proovi töö ära teha ja sule ühendus igal juhul.
    try:
        print("ETL etapp 1/3: Andmete vastuvõtt ja laadimine staging kihti")
        api_users = fetch_api_users()

        # `f"..."` on f-string.
        # See lubab panna muutuja väärtuse otse teksti sisse.
        print(f"- API-st tuli {len(api_users)} kasutajat.")
        load_api_users(conn, api_users)
        print(f"- Laadisin staging.api_users tabelisse {len(api_users)} rida.")
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


# See plokk käivitab `main()` ainult siis, kui paneme selle faili otse jooksma.
if __name__ == "__main__":
    main()