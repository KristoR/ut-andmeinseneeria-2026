"""Kontrolli, kas staging-tabelite võtmed sobituvad.

See skript ei muuda andmeid.
Ta loeb võtmed andmebaasist, puhastab need võrreldavale kujule ja näitab,
millised võtmed kattuvad ja millised jäävad kummaski allikas üksi.
"""

import os

import psycopg2
from psycopg2 import sql


def get_connection():
    """Loo andmebaasiühendus keskkonnamuutujate põhjal."""

    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "db"),
        port=os.environ.get("DB_PORT", "5432"),
        user=os.environ.get("DB_USER", "praktikum"),
        password=os.environ.get("DB_PASSWORD", "praktikum"),
        dbname=os.environ.get("DB_NAME", "praktikum"),
    )


def split_relation_name(relation_name):
    """Jaga nimi kujul `skeem.objekt` kaheks eraldi osaks."""

    schema_name, table_name = relation_name.split(".", maxsplit=1)
    return schema_name, table_name


def relation_exists(conn, relation_name):
    """Kontrolli, kas antud tabel või vaade on andmebaasis olemas."""

    with conn.cursor() as cur:
        # `to_regclass` tagastab objekti nime, kui see leidub,
        # ja `NULL`, kui seda ei ole olemas.
        cur.execute("SELECT to_regclass(%s);", (relation_name,))
        return cur.fetchone()[0] is not None


def fetch_normalized_keys(conn, relation_name, key_column):
    """Loe tabelist välja puhastatud võtmed.

    Puhastamine toimub SQL-is:
    - `TRIM(...)` eemaldab tühikud algusest ja lõpust;
    - `LOWER(...)` muudab teksti väikesteks tähtedeks.

    Nii võrdleme võtmeid samal kujul, isegi kui allikates oli kirjapilt erinev.
    """

    schema_name, table_name = split_relation_name(relation_name)

    # Siin kasutame `psycopg2.sql` moodulit, sest tabeli- ja veerunimesid
    # ei saa turvaliselt panna SQL-i sisse tavaliste `%s` parameetritega.
    # `sql.Identifier(...)` märgib, et tegemist on SQL-identifikaatoriga.
    query = sql.SQL(
        """
        SELECT DISTINCT LOWER(TRIM({key_column}::text)) AS key_value
        FROM {schema_name}.{table_name}
        WHERE {key_column} IS NOT NULL
        ORDER BY key_value;
        """
    ).format(
        schema_name=sql.Identifier(schema_name),
        table_name=sql.Identifier(table_name),
        key_column=sql.Identifier(key_column),
    )

    with conn.cursor() as cur:
        cur.execute(query)
        return [row[0] for row in cur.fetchall()]


def compare_key_sets(left_keys, right_keys):
    """Võrdle kahte võtmehulkade loendit.

    `set` sobib siia hästi, sest hulga abil on lihtne leida:
    - ühised võtmed;
    - ainult vasakus allikas olevad võtmed;
    - ainult paremas allikas olevad võtmed.
    """

    left_key_set = set(left_keys)
    right_key_set = set(right_keys)

    return {
        "matched_keys": sorted(left_key_set & right_key_set),
        "left_only_keys": sorted(left_key_set - right_key_set),
        "right_only_keys": sorted(right_key_set - left_key_set),
    }


def print_check_keys(
    conn,
    source_a,
    source_b,
    key,
    label_a,
    label_b,
):
    """Prindi kahe allika võtmete võrdlus inimesele loetaval kujul."""

    # Kõigepealt loeme mõlemast allikast sama võtme ühetaolisel kujul välja.
    left_keys = fetch_normalized_keys(conn, source_a, key)
    right_keys = fetch_normalized_keys(conn, source_b, key)

    # Seejärel võrdleme kahte võtmehulkade loendit omavahel.
    comparison = compare_key_sets(left_keys, right_keys)

    print(f"- Võrdlus: {label_a} ja {label_b}")
    print(f"- Sobitunud e-posti võtmeid: {len(comparison['matched_keys'])}")
    print(
        f"- {label_a} poolel ilma {label_b} vasteta: "
        f"{len(comparison['left_only_keys'])}"
    )
    for key_value in comparison["left_only_keys"]:
        print(f"  {label_a} ainult: {key_value}")

    print(
        f"- {label_b} poolel ilma {label_a} vasteta: "
        f"{len(comparison['right_only_keys'])}"
    )
    for key_value in comparison["right_only_keys"]:
        print(f"  {label_b} ainult: {key_value}")


def main():
    """Käivita võtmekontroll põhiraja ja vajadusel lisaallika jaoks."""

    conn = get_connection()
    try:
        # Põhirajal võrdleme API ja CSV allikat.
        print_check_keys(
            conn=conn,
            source_a="staging.api_users",
            source_b="staging.user_status",
            key="email",
            label_a="API",
            label_b="CSV",
        )

        # Kui õppija on teinud lisaülesande 1, on olemas ka JSON-i staging-tabel.
        if relation_exists(conn, "staging.notification_preferences"):
            print_check_keys(
                conn=conn,
                source_a="staging.api_users",
                source_b="staging.notification_preferences",
                key="email",
                label_a="API",
                label_b="JSON",
            )
    finally:
        conn.close()


# See plokk käivitab `main()` ainult siis, kui paneme faili otse jooksma.
if __name__ == "__main__":
    main()
