"""cron-põhise praktikumi orkestreerija.

See fail on praktikumi peamine töövooskript.

Skripti loogika on jagatud väikesteks sammudeks:

1. loe käsurea käsk sisse;
2. otsusta, kas töötleme ühte päeva, mitut päeva või järgmist puuduva päeva;
3. lae vajadusel või eraldi käsuga dimensioonid;
4. päri tellimused API-st;
5. lae tellimused `staging` kihti;
6. ehita `analytics` kihti valitud päeva koondread uuesti;
7. kirjuta iga sammu kohta logirida.

Kui Python on sulle uus, loe faili selles järjekorras:

1. `main()`
2. `parse_args()`
3. `run_pipeline_for_date()`
4. seejärel väiksemad abifunktsioonid
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg2
import requests


TALLINN_TZ = ZoneInfo("Europe/Tallinn")
SOURCE_DATA_DIR = Path("/app/source_data")
CHECK_SQL_PATH = Path("/app/scripts/01_check_pipeline.sql")
SOURCE_API_URL = "http://source-api:8014"
SOURCE_START_DATE = date(2026, 4, 1)
SOURCE_END_DATE = date(2026, 4, 9)
SOURCE_BUSINESS_DATE = date(2026, 4, 9)
MAX_ATTEMPTS = 3
# Need vaikimisi väärtused hoiame faili alguses koos.
# Nii on õppijal lihtne näha, millest skript oma tööks lähtub.


def now_local() -> datetime:
    """Tagasta praegune aeg Tallinna ajavööndis."""
    # `return` lõpetab funktsiooni ja annab väärtuse tagasi sinna,
    # kust see funktsioon välja kutsuti.
    return datetime.now(TALLINN_TZ)


def log(message: str) -> None:
    """Prindi üks logirida terminali või logifaili."""
    print(f"{now_local().isoformat()} | {message}", flush=True)


def get_connection():
    """Loo ühendus praktikumi andmebaasiga.

    Ühenduse väärtused tulevad keskkonnamuutujatest. Nii saab sama skript
    töötada eri keskkondades ilma, et peaks koodi ennast muutma.
    """
    # Ka siin kasutame `return` lauset: anname valmis ühendusobjekti tagasi.
    return psycopg2.connect(
        host=_get_env("DB_HOST", "db"),
        port=_get_env("DB_PORT", "5432"),
        user=_get_env("DB_USER", "praktikum"),
        password=_get_env("DB_PASSWORD", "praktikum"),
        dbname=_get_env("DB_NAME", "praktikum"),
    )


def _get_env(name: str, default: str) -> str:
    """Küsi keskkonnamuutuja väärtust või kasuta vaikimisi väärtust."""
    import os

    return os.environ.get(name, default)


def get_source_api_url() -> str:
    """Tagasta kohaliku API baas-URL."""
    return _get_env("SOURCE_API_URL", SOURCE_API_URL).rstrip("/")


def get_source_range() -> tuple[date, date]:
    """Tagasta kuupäevavahemik, mille kohta kohalik API andmeid pakub."""
    start = date.fromisoformat(_get_env("SOURCE_START_DATE", SOURCE_START_DATE.isoformat()))
    end = date.fromisoformat(_get_env("SOURCE_END_DATE", SOURCE_END_DATE.isoformat()))
    return start, end


def get_business_date() -> date:
    """Tagasta praktikumi aktiivne ärikuupäev.

    See on kuupäev, mille andmed võivad veel päeva jooksul muutuda.
    Varasemad päevad loeme valmis päevadeks.
    """
    return date.fromisoformat(
        _get_env("SOURCE_BUSINESS_DATE", SOURCE_BUSINESS_DATE.isoformat())
    )


def get_finalized_through() -> date | None:
    """Tagasta viimane päev, mida võib pidada lõpetatuks."""
    start_date, end_date = get_source_range()
    finalized_through = min(end_date, get_business_date() - timedelta(days=1))
    if finalized_through < start_date:
        # `None` tähendab siin: veel ei ole ühtegi päeva, mida saaks valmis päevaks pidada.
        return None
    return finalized_through


def ensure_date_in_source_range(logical_date: date) -> None:
    """Kontrolli, et kasutaja küsitud kuupäev on olemasoleva allika vahemikus."""
    start, end = get_source_range()
    if logical_date < start or logical_date > end:
        raise ValueError(
            "Kuupäev peab jääma vahemikku "
            f"{start.isoformat()} kuni {end.isoformat()}."
        )


def insert_log_row(
    conn,
    *,
    run_id: uuid.UUID,
    step_name: str,
    logical_date: date | None,
    attempt_no: int,
    status: str,
    rows_loaded: int,
    message: str,
    started_at: datetime,
    finished_at: datetime,
) -> None:
    """Kirjuta üks logikirje logitabelisse `staging.pipeline_run_log`.

    See on praktikumi logitabel. Hiljem saab SQL-iga vaadata:

    - mis samm jooksis;
    - millise päeva kohta;
    - kas see oli esimene, teine või kolmas katse;
    - kas samm õnnestus, ebaõnnestus või jäeti vahele.
    """
    # `with` hoolitseb selle eest, et kursor suletakse ploki lõpus automaatselt.
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO staging.pipeline_run_log (
                run_id,
                step_name,
                logical_date,
                attempt_no,
                status,
                rows_loaded,
                message,
                started_at,
                finished_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                str(run_id),
                step_name,
                logical_date,
                attempt_no,
                status,
                rows_loaded,
                message,
                started_at,
                finished_at,
            ),
        )
    # `commit` teeb muudatused andmebaasis püsivaks.
    conn.commit()


def log_skip(conn, *, run_id: uuid.UUID, step_name: str, logical_date: date | None, message: str) -> None:
    """Kirjuta logitabelisse rida olukorra kohta, kus samm jäeti teadlikult vahele."""
    started_at = now_local()
    finished_at = now_local()
    insert_log_row(
        conn,
        run_id=run_id,
        step_name=step_name,
        logical_date=logical_date,
        attempt_no=1,
        status="skipped",
        rows_loaded=0,
        message=message,
        started_at=started_at,
        finished_at=finished_at,
    )
    log(f"{step_name}: {message}")


def table_has_rows(conn, table_name: str) -> bool:
    """Kontrolli, kas etteantud tabelis on vähemalt üks rida."""
    with conn.cursor() as cur:
        # `SELECT EXISTS` tagastab tõeväärtuse:
        # kas vähemalt üks sobiv rida leidus või mitte.
        cur.execute(f"SELECT EXISTS (SELECT 1 FROM {table_name} LIMIT 1);")
        return bool(cur.fetchone()[0])


def load_products(conn, run_id: uuid.UUID, logical_date: date | None) -> int:
    """Lae `products.csv` sisu tabelisse `staging.products_raw`.

    `logical_date` võib siin olla ka `None`.
    See juhtub siis, kui värskendame dimensioone eraldi käsuga, ilma ühegi
    kindla tellimuste päeva töötluseta.
    """
    started_at = now_local()
    # `try` tähendab: proovi allolev töö ära teha.
    # Kui siin plokis tekib viga, liigub Python vastava `except` plokini.
    try:
        # `with` avab faili ja sulgeb selle töö lõpus automaatselt.
        with (SOURCE_DATA_DIR / "products.csv").open(encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        with conn.cursor() as cur:
            # `for` käib read ükshaaval läbi.
            for row in rows:
                # `DictReader` tagastab iga rea sõnastikuna.
                # Seetõttu saame väärtusi võtta nime järgi, näiteks `row["product_id"]`.
                cur.execute(
                    """
                    INSERT INTO staging.products_raw (
                        product_id,
                        product_name,
                        category,
                        base_price_eur,
                        loaded_at
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (product_id) DO UPDATE SET
                        product_name = EXCLUDED.product_name,
                        category = EXCLUDED.category,
                        base_price_eur = EXCLUDED.base_price_eur,
                        loaded_at = EXCLUDED.loaded_at
                    """,
                    (
                        row["product_id"],
                        row["product_name"],
                        row["category"],
                        Decimal(row["base_price_eur"]),
                        now_local(),
                    ),
                )
        # Kui kõik read said edukalt sisse, salvestame muudatused andmebaasi.
        conn.commit()
        finished_at = now_local()
        message = f"Laadisin staging.products_raw tabelisse {len(rows)} toodet."
        insert_log_row(
            conn,
            run_id=run_id,
            step_name="load_products",
            logical_date=logical_date,
            attempt_no=1,
            status="success",
            rows_loaded=len(rows),
            message=message,
            started_at=started_at,
            finished_at=finished_at,
        )
        log(message)
        # Anname kutsuvale funktsioonile tagasi, mitu rida laadisime.
        return len(rows)
    except Exception as exc:
        # `except` käivitub siis, kui `try` plokis läks midagi valesti.
        # `rollback` tühistab pooleliolevad andmebaasimuudatused.
        conn.rollback()
        finished_at = now_local()
        message = f"Toodete laadimine ebaõnnestus: {exc}"
        insert_log_row(
            conn,
            run_id=run_id,
            step_name="load_products",
            logical_date=logical_date,
            attempt_no=1,
            status="error",
            rows_loaded=0,
            message=message,
            started_at=started_at,
            finished_at=finished_at,
        )
        log(message)
        raise


def load_stores(conn, run_id: uuid.UUID, logical_date: date | None) -> int:
    """Lae `stores.csv` sisu tabelisse `staging.stores_raw`.

    `logical_date` võib siin olla ka `None`.
    See juhtub siis, kui värskendame dimensioone eraldi käsuga.
    """
    started_at = now_local()
    try:
        with (SOURCE_DATA_DIR / "stores.csv").open(encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        with conn.cursor() as cur:
            # Loogika on sama mis toodete laadimisel:
            # loeme read läbi ja kirjutame need ükshaaval tabelisse.
            for row in rows:
                cur.execute(
                    """
                    INSERT INTO staging.stores_raw (
                        store_id,
                        store_name,
                        city,
                        region,
                        loaded_at
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (store_id) DO UPDATE SET
                        store_name = EXCLUDED.store_name,
                        city = EXCLUDED.city,
                        region = EXCLUDED.region,
                        loaded_at = EXCLUDED.loaded_at
                    """,
                    (
                        row["store_id"],
                        row["store_name"],
                        row["city"],
                        row["region"],
                        now_local(),
                    ),
                )
        conn.commit()
        finished_at = now_local()
        message = f"Laadisin staging.stores_raw tabelisse {len(rows)} poodi."
        insert_log_row(
            conn,
            run_id=run_id,
            step_name="load_stores",
            logical_date=logical_date,
            attempt_no=1,
            status="success",
            rows_loaded=len(rows),
            message=message,
            started_at=started_at,
            finished_at=finished_at,
        )
        log(message)
        return len(rows)
    except Exception as exc:
        # Kui ükski poodide laadimise osa ebaõnnestub, jätame poolelioleva laadimise andmebaasi salvestamata.
        conn.rollback()
        finished_at = now_local()
        message = f"Poodide laadimine ebaõnnestus: {exc}"
        insert_log_row(
            conn,
            run_id=run_id,
            step_name="load_stores",
            logical_date=logical_date,
            attempt_no=1,
            status="error",
            rows_loaded=0,
            message=message,
            started_at=started_at,
            finished_at=finished_at,
        )
        log(message)
        raise


def maybe_load_dimensions(
    conn, run_id: uuid.UUID, logical_date: date | None, force_refresh: bool
) -> None:
    """Otsusta, kas dimensioonid tuleb uuesti laadida.

    Reegel on lihtne:

    - kui käivitasime eraldi käsu `refresh-dimensions`, siis laeme;
    - kui tabelid on tühjad, siis laeme;
    - muul juhul jätame sammu vahele ja logime selle otsuse.
    """
    dimensions_present = table_has_rows(conn, "staging.products_raw") and table_has_rows(
        conn, "staging.stores_raw"
    )
    # `if` tähendab: kui tingimus on tõene, tee see haru.
    if force_refresh or not dimensions_present:
        reason = "Käivitasin eraldi dimensioonide värskenduse" if force_refresh else "Dimensioonid puudusid"
        log(f"{reason}. Laen tooted ja poed staging kihti.")
        load_products(conn, run_id, logical_date)
        load_stores(conn, run_id, logical_date)
        return

    log_skip(
        conn,
        run_id=run_id,
        step_name="load_products",
        logical_date=logical_date,
        message="Tooted on juba olemas. Jätan laadimise vahele.",
    )
    log_skip(
        conn,
        run_id=run_id,
        step_name="load_stores",
        logical_date=logical_date,
        message="Poed on juba olemas. Jätan laadimise vahele.",
    )


def run_refresh_dimensions(conn) -> int:
    """Lae dimensioonid eraldi käsuga uuesti.

    See käsk ei päri tellimusi API-st ja ei ehita analytics kihti.
    Ta värskendab ainult tabelid `staging.products_raw` ja
    `staging.stores_raw`.
    """
    run_id = uuid.uuid4()
    log(f"Alustan dimensioonide värskendamist (run_id={run_id}).")
    maybe_load_dimensions(conn, run_id, None, True)
    log("Dimensioonide värskendamine sai valmis.")
    return 0


def fetch_orders_from_api(logical_date: date, source_mode: str, run_id: uuid.UUID) -> list[dict]:
    """Päri ühe päeva tellimused kohalikust API-st.

    See funktsioon teeb ainult HTTP päringu.
    Retry loogika on eraldi funktsioonis `extract_orders_with_retry()`.
    """
    # `requests.get(...)` saadab HTTP GET päringu.
    # `params` sõnastikust teeb teek päringurea kujul `?date=...&mode=...`.
    response = requests.get(
        f"{get_source_api_url()}/api/orders",
        params={"date": logical_date.isoformat(), "mode": source_mode},
        headers={"X-Run-Id": str(run_id)},
        timeout=10,
    )

    try:
        # `response.json()` muudab JSON-vastuse Pythonis sõnastikuks või loendiks.
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"API vastus ei olnud korrektne JSON: {exc}") from exc

    # Kui API vastas veakoodiga, muudame selle Pythonis veaks.
    # See aitab retry loogikal otsustada, kas proovida uuesti.
    if response.status_code >= 400:
        error_message = payload.get("message", "Tundmatu API viga.")
        error_text = f"source-api vastas koodiga {response.status_code}: {error_message}"

        # Kõik vead ei ole ajutised.
        # Näiteks tuleviku kuupäeva või vale sisendi puhul ei ole mõtet kolm korda
        # sama päringut korrata. Sellisel juhul katkestame kohe.
        if 400 <= response.status_code < 500 and response.status_code != 429:
            # `raise` tekitab Pythonis vea.
            # Siin ütleme teadlikult: see on püsiv viga, ära proovi uuesti.
            raise ValueError(error_text)

        raise RuntimeError(error_text)

    orders = payload.get("orders")
    if not isinstance(orders, list):
        raise RuntimeError("API vastuses puudus orders loend.")
    # Kui kõik kontrollid läksid hästi, anname tellimuste loendi tagasi.
    return orders


def extract_orders_with_retry(
    conn, run_id: uuid.UUID, logical_date: date, source_mode: str
) -> list[dict]:
    """Püüa tellimused API-st kätte saada kuni kolm korda.

    Kui esimene katse ebaõnnestub, ootame natuke ja proovime uuesti.
    Ooteaeg kasvab iga korraga.
    """
    # `range(1, MAX_ATTEMPTS + 1)` annab väärtused 1, 2, 3.
    # `for` kordab sama loogikat iga katse jaoks.
    for attempt_no in range(1, MAX_ATTEMPTS + 1):
        started_at = now_local()
        try:
            log(
                f"extract_orders: katse {attempt_no}/{MAX_ATTEMPTS} "
                f"kuupäevale {logical_date.isoformat()}."
            )
            orders = fetch_orders_from_api(logical_date, source_mode, run_id)
            finished_at = now_local()
            message = (
                f"Pärisin kohalikust API-st {len(orders)} tellimust "
                f"kuupäeva {logical_date.isoformat()} jaoks."
            )
            insert_log_row(
                conn,
                run_id=run_id,
                step_name="extract_orders",
                logical_date=logical_date,
                attempt_no=attempt_no,
                status="success",
                rows_loaded=len(orders),
                message=message,
                started_at=started_at,
                finished_at=finished_at,
            )
            log(message)
            # Eduka katse korral lõpetame funktsiooni kohe.
            return orders
        except ValueError as exc:
            # `ValueError` tähendab siin püsivat sisendiviga,
            # näiteks tuleviku kuupäeva. Seda ei ole mõtet uuesti proovida.
            conn.rollback()
            finished_at = now_local()
            message = str(exc)
            insert_log_row(
                conn,
                run_id=run_id,
                step_name="extract_orders",
                logical_date=logical_date,
                attempt_no=attempt_no,
                status="error",
                rows_loaded=0,
                message=message,
                started_at=started_at,
                finished_at=finished_at,
            )
            log(f"extract_orders katkestati ilma uue katsena: {message}")
            raise
        except Exception as exc:
            # Siia jõuavad ülejäänud vead.
            # Neid käsitleme ajutise probleemina kuni katsed otsa saavad.
            conn.rollback()
            finished_at = now_local()
            message = str(exc)
            insert_log_row(
                conn,
                run_id=run_id,
                step_name="extract_orders",
                logical_date=logical_date,
                attempt_no=attempt_no,
                status="error",
                rows_loaded=0,
                message=message,
                started_at=started_at,
                finished_at=finished_at,
            )
            log(f"extract_orders ebaõnnestus: {message}")
            if attempt_no == MAX_ATTEMPTS:
                # Viimase katse järel anname vea edasi ja töövoog katkeb.
                raise

            # See annab ooteajad 1, 2 ja 4 sekundit.
            sleep_seconds = 2 ** (attempt_no - 1)
            log(f"Ootan {sleep_seconds} sekundit ja proovin uuesti.")
            time.sleep(sleep_seconds)

    raise RuntimeError("Retry loogika jõudis ootamatu lõppu.")


def parse_api_timestamp(value: str) -> datetime:
    """Muuda API ajatempel Python `datetime` objektiks."""
    # API võib kasutada lõpus tähte `Z`, mis tähendab UTC ajavööndit.
    # `fromisoformat` ootab siin kuju `+00:00`, seetõttu vahetame selle enne ära.
    normalized = value.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized)


def load_orders(conn, run_id: uuid.UUID, logical_date: date, orders: list[dict]) -> int:
    """Lae API-st saadud tellimused tabelisse `staging.orders_raw`."""
    started_at = now_local()
    try:
        with conn.cursor() as cur:
            # Käime API-st saadud tellimused ükshaaval läbi ja salvestame staging kihti.
            for order in orders:
                # Kui sama `order_id` on juba olemas, siis uuendame seda rida.
                # Nii ei teki sama päeva korduskäivitamisel duplikaate.
                cur.execute(
                    """
                    INSERT INTO staging.orders_raw (
                        order_id,
                        order_date,
                        store_id,
                        product_id,
                        quantity,
                        unit_price_eur,
                        source_updated_at,
                        loaded_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (order_id) DO UPDATE SET
                        order_date = EXCLUDED.order_date,
                        store_id = EXCLUDED.store_id,
                        product_id = EXCLUDED.product_id,
                        quantity = EXCLUDED.quantity,
                        unit_price_eur = EXCLUDED.unit_price_eur,
                        source_updated_at = EXCLUDED.source_updated_at,
                        loaded_at = EXCLUDED.loaded_at
                    """,
                    (
                        order["order_id"],
                        logical_date,
                        order["store_id"],
                        order["product_id"],
                        int(order["quantity"]),
                        Decimal(str(order["unit_price_eur"])),
                        parse_api_timestamp(order["source_updated_at"]),
                        now_local(),
                    ),
                )
        conn.commit()
        finished_at = now_local()
        message = (
            f"Laadisin staging.orders_raw tabelisse {len(orders)} tellimust "
            f"kuupäeva {logical_date.isoformat()} jaoks."
        )
        insert_log_row(
            conn,
            run_id=run_id,
            step_name="load_orders",
            logical_date=logical_date,
            attempt_no=1,
            status="success",
            rows_loaded=len(orders),
            message=message,
            started_at=started_at,
            finished_at=finished_at,
        )
        log(message)
        return len(orders)
    except Exception as exc:
        # Kui laadimine jäi pooleli, tühistame selle päeva pooleliolevad muudatused.
        conn.rollback()
        finished_at = now_local()
        message = f"Tellimuste laadimine ebaõnnestus: {exc}"
        insert_log_row(
            conn,
            run_id=run_id,
            step_name="load_orders",
            logical_date=logical_date,
            attempt_no=1,
            status="error",
            rows_loaded=0,
            message=message,
            started_at=started_at,
            finished_at=finished_at,
        )
        log(message)
        raise


def rebuild_analytics_for_date(conn, run_id: uuid.UUID, logical_date: date) -> int:
    """Ehita ühe päeva koondread uuesti tabelisse `analytics.daily_product_sales`.

    Siin ei arvuta me tulemust üle kõigi päevade korraga.
    Iga käivitus kustutab ja ehitab uuesti ainult etteantud päeva read.
    """
    started_at = now_local()
    rebuilt_at = now_local()
    try:
        with conn.cursor() as cur:
            # Eemaldame ainult selle päeva vanad read, mida kohe uuesti ehitame.
            cur.execute(
                "DELETE FROM analytics.daily_product_sales WHERE sales_date = %s;",
                (logical_date,),
            )
            cur.execute(
                """
                INSERT INTO analytics.daily_product_sales (
                    sales_date,
                    store_id,
                    store_name,
                    region,
                    product_id,
                    product_name,
                    category,
                    order_count,
                    total_quantity,
                    gross_sales_eur,
                    rebuilt_at
                )
                SELECT
                    order_date AS sales_date,
                    store_id,
                    COALESCE(store_name, 'Tundmatu pood') AS store_name,
                    COALESCE(region, 'Tundmatu piirkond') AS region,
                    product_id,
                    COALESCE(product_name, 'Tundmatu toode') AS product_name,
                    COALESCE(category, 'Määramata') AS category,
                    COUNT(*) AS order_count,
                    SUM(quantity) AS total_quantity,
                    ROUND(SUM(total_amount_eur), 2) AS gross_sales_eur,
                    %s AS rebuilt_at
                FROM intermediate.orders_enriched
                WHERE order_date = %s
                GROUP BY
                    order_date,
                    store_id,
                    COALESCE(store_name, 'Tundmatu pood'),
                    COALESCE(region, 'Tundmatu piirkond'),
                    product_id,
                    COALESCE(product_name, 'Tundmatu toode'),
                    COALESCE(category, 'Määramata')
                ORDER BY store_id, product_id
                """,
                (rebuilt_at, logical_date),
            )
            # `rowcount` näitab, mitu rida viimane SQL käsk lisas.
            row_count = cur.rowcount
        conn.commit()
        finished_at = now_local()
        message = (
            f"Ehitasin analytics.daily_product_sales tabelisse {row_count} koondrida "
            f"kuupäeva {logical_date.isoformat()} jaoks."
        )
        insert_log_row(
            conn,
            run_id=run_id,
            step_name="build_analytics",
            logical_date=logical_date,
            attempt_no=1,
            status="success",
            rows_loaded=row_count,
            message=message,
            started_at=started_at,
            finished_at=finished_at,
        )
        log(message)
        return row_count
    except Exception as exc:
        # Ka analytics kihi puhul tühistame pooleli jäänud muudatused,
        # et sama päev ei jääks pooleldi uuendatud seisu.
        conn.rollback()
        finished_at = now_local()
        message = f"Analytics kihi uuendamine ebaõnnestus: {exc}"
        insert_log_row(
            conn,
            run_id=run_id,
            step_name="build_analytics",
            logical_date=logical_date,
            attempt_no=1,
            status="error",
            rows_loaded=0,
            message=message,
            started_at=started_at,
            finished_at=finished_at,
        )
        log(message)
        raise


def run_pipeline_for_date(
    conn,
    *,
    logical_date: date,
    source_mode: str,
) -> None:
    """Käivita kogu töövoog ühe loogilise kuupäeva jaoks.

    Kui tahad aru saada, mis järjekorras sammud jooksevad, alusta lugemist siit.
    """
    ensure_date_in_source_range(logical_date)
    # `run_id` seob sama käivituse logiread omavahel kokku.
    run_id = uuid.uuid4()
    log(
        f"Alustan töövoogu kuupäevale {logical_date.isoformat()} "
        f"(run_id={run_id}, source_mode={source_mode})."
    )
    # 1. vajadusel maandame dimensioonid staging kihti
    maybe_load_dimensions(conn, run_id, logical_date, False)
    # 2. pärime päevased tellimused API-st
    orders = extract_orders_with_retry(conn, run_id, logical_date, source_mode)
    # 3. kirjutame tellimused staging kihti
    load_orders(conn, run_id, logical_date, orders)
    # 4. ehitame selle päeva koondread analytics kihti uuesti
    rebuild_analytics_for_date(conn, run_id, logical_date)
    log(f"Töövoog kuupäevale {logical_date.isoformat()} sai valmis.")


def daterange(start_date: date, end_date: date):
    """Anna kuupäevad ükshaaval algusest lõpuni."""
    current = start_date
    while current <= end_date:
        # `yield` tähendab: anna üks väärtus välja, aga ära lõpeta funktsiooni lõplikult.
        # Järgmise küsimise korral jätkab Python samast kohast.
        yield current
        current += timedelta(days=1)


def get_successful_analytics_dates(conn) -> set[date]:
    """Tagasta need päevad, mille kogu päevatulemus on juba edukalt valmis ehitatud.

    Valmisoleku aluseks ei ole ainult tellimuste sissetulek.
    Päev loeme tehtuks alles siis, kui ka `analytics` kiht sai edukalt valmis.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT logical_date
            FROM staging.pipeline_run_log
            WHERE step_name = 'build_analytics'
              AND status = 'success'
              AND logical_date IS NOT NULL
            """
        )
        successful_dates = set()
        # `fetchall()` tagastab kõik tulemuse read korraga.
        # Iga rida on siin ühe väljaga tuple, näiteks `(date(2026, 4, 1),)`.
        for row in cur.fetchall():
            successful_dates.add(row[0])
        return successful_dates


def find_next_missing_finalized_date(conn) -> date | None:
    """Leia järgmine lõpetatud päev, mida pole veel edukalt töödeldud."""
    start_date, _ = get_source_range()
    finalized_through = get_finalized_through()
    if finalized_through is None:
        return None

    successful_dates = get_successful_analytics_dates(conn)
    for candidate in daterange(start_date, finalized_through):
        if candidate not in successful_dates:
            return candidate
    return None


def run_scheduled(conn) -> int:
    """Käivita scheduler'i vaikeloogika.

    `cron` kutsub seda käsku ilma kuupäeva kaasa andmata.
    Seepärast peab skript ise otsustama, milline päev järgmisena töödelda.
    """
    next_date = find_next_missing_finalized_date(conn)
    if next_date is not None:
        # Eelistame alati kõigepealt ajaloolise augu täitmist.
        log(
            "Leidsin puuduva valmis päeva. "
            f"Töötlen kõigepealt kuupäeva {next_date.isoformat()}."
        )
        run_pipeline_for_date(
            conn,
            logical_date=next_date,
            source_mode="stable",
        )
        return 0

    start_date, end_date = get_source_range()
    business_date = get_business_date()

    if start_date <= business_date <= end_date:
        # Kui kõik varasemad valmis päevad on tehtud,
        # hoiame aktiivset äripäeva korduskäivitustega ajakohasena.
        log(
            "Kõik valmis päevad on laetud. "
            f"Töötlen aktiivset äripäeva {business_date.isoformat()} uuesti üle."
        )
        run_pipeline_for_date(
            conn,
            logical_date=business_date,
            source_mode="stable",
        )
        return 0

    if business_date < start_date:
        run_id = uuid.uuid4()
        log_skip(
            conn,
            run_id=run_id,
            step_name="run_scheduled",
            logical_date=None,
            message=(
                "Aktiivne äripäev ei ole veel allika vahemikku jõudnud. "
                "Praegu pole midagi töödelda."
            ),
        )
        return 0

    run_id = uuid.uuid4()
    log_skip(
        conn,
        run_id=run_id,
        step_name="run_scheduled",
        logical_date=None,
        message=(
            "Pole uut kuupäeva töödelda. Kõik päevad vahemikus "
            f"{start_date.isoformat()} kuni {end_date.isoformat()} "
            "on juba valmis."
        ),
    )
    return 0


def run_backfill(conn, from_date: date, to_date: date) -> int:
    """Töötle mitu päeva järjest vanemast uuemani."""
    if from_date > to_date:
        raise ValueError("--from-date peab olema varasem või sama mis --to-date.")
    ensure_date_in_source_range(from_date)
    ensure_date_in_source_range(to_date)

    # `for` käib kuupäevad läbi vanemast uuemani.
    for logical_date in daterange(from_date, to_date):
        run_pipeline_for_date(
            conn,
            logical_date=logical_date,
            source_mode="stable",
        )
    return 0


def parse_args() -> argparse.Namespace:
    """Loe käsurea argumendid sisse.

    `argparse` aitab teha käske kujul:

    - `refresh-dimensions`
    - `run-once --logical-date YYYY-MM-DD`
    - `run-scheduled`
    - `backfill --from-date YYYY-MM-DD --to-date YYYY-MM-DD`
    """
    # `ArgumentParser` loeb käsurea käsud ja lipud lahti.
    parser = argparse.ArgumentParser(
        description="cron-põhise praktikumi orkestreerija."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser(
        "refresh-dimensions",
        help="Lae toote- ja poedimensioonid eraldi uuesti.",
    )

    run_once = subparsers.add_parser(
        "run-once", help="Käivita töövoog ühele loogilisele kuupäevale."
    )
    run_once.add_argument("--logical-date", required=True, type=date.fromisoformat)
    run_once.add_argument("--source-mode", choices=["stable", "fail_once"], default="stable")

    subparsers.add_parser(
        "run-scheduled",
        help="Täida puuduva valmis päev või töötle aktiivne äripäev uuesti üle.",
    )

    backfill = subparsers.add_parser("backfill", help="Töötle kuupäevavahemik järjest läbi.")
    backfill.add_argument("--from-date", required=True, type=date.fromisoformat)
    backfill.add_argument("--to-date", required=True, type=date.fromisoformat)

    subparsers.add_parser("show-check-sql", help="Näita, kus kontrollpäringute fail asub.")

    return parser.parse_args()


def main() -> int:
    """Skripti peamine sisenemispunkt."""
    args = parse_args()

    if args.command == "show-check-sql":
        log(f"Kontrollpäringute fail asub siin: {CHECK_SQL_PATH}")
        return 0

    # Ühendume andmebaasiga ühe korra ja kasutame sama ühendust terve käsu jooksul.
    conn = get_connection()
    try:
        if args.command == "refresh-dimensions":
            return run_refresh_dimensions(conn)

        if args.command == "run-once":
            run_pipeline_for_date(
                conn,
                logical_date=args.logical_date,
                source_mode=args.source_mode,
            )
            return 0

        if args.command == "run-scheduled":
            return run_scheduled(conn)

        if args.command == "backfill":
            return run_backfill(conn, args.from_date, args.to_date)

        raise ValueError(f"Tundmatu käsk: {args.command}")
    except Exception as exc:
        # Kui viga jõuab siiani, siis ei saadud seda madalamal tasemel enam lahendada.
        log(f"Töövoog ebaõnnestus: {exc}")
        return 1
    finally:
        # `finally` käivitub alati, ka siis kui enne tekkis viga.
        # See on hea koht ühenduse sulgemiseks.
        conn.close()


if __name__ == "__main__":
    # See plokk tähendab:
    # käivita `main()` ainult siis, kui paneme selle faili otse jooksma.
    sys.exit(main())
