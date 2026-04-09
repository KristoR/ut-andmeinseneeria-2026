"""Kohalik API päevatellimuste simuleerimiseks.

Selle faili eesmärk on anda praktikumi jaoks väike ja kontrollitav andmeallikas.

Miks see on kasulik?

- me ei sõltu välistest teenustest;
- sama kuupäev annab alati sama vastuse;
- saame simuleerida ajutist tõrget `fail_once`, et retry loogikat proovida.
"""

from __future__ import annotations

import json
import os
from math import cos, log, pi, sqrt
from datetime import date, datetime, timedelta
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo


HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", "8014"))
TALLINN_TZ = ZoneInfo(os.environ.get("TZ", "Europe/Tallinn"))
SOURCE_START_DATE = date.fromisoformat(os.environ.get("SOURCE_START_DATE", "2026-04-01"))
SOURCE_END_DATE = date.fromisoformat(os.environ.get("SOURCE_END_DATE", "2026-04-09"))
SOURCE_BUSINESS_DATE = date.fromisoformat(
    os.environ.get("SOURCE_BUSINESS_DATE", SOURCE_END_DATE.isoformat())
)
# Hoidame allika kuupäevakonteksti faili alguses koos.
# Nii on lihtsam näha, millist "ärilist tänast päeva" server parajasti simuleerib.
SEED_PREFIX = "praktikum-04-base-source-api"
AVERAGE_ORDERS_PER_DAY = 100
ORDERS_PER_DAY_STDDEV = 10
ORDER_COUNT_MIN = 60
ORDER_COUNT_MAX = 140
ORDER_TIME_MEAN_MINUTES = 18 * 60
ORDER_TIME_STDDEV_MINUTES = 2 * 60
MINUTES_PER_DAY = 24 * 60
FAIL_ONCE_DIR = Path("/tmp/source_api_fail_once")
FAIL_ONCE_DIR.mkdir(parents=True, exist_ok=True)

PRODUCTS = [
    {"product_id": "P-100", "product_name": "Terasest veepudel", "base_price_eur": 24.90},
    {"product_id": "P-200", "product_name": "Juhtmevaba laadija", "base_price_eur": 34.50},
    {"product_id": "P-300", "product_name": "Laualamp", "base_price_eur": 49.00},
    {"product_id": "P-400", "product_name": "Märkmik A5", "base_price_eur": 6.80},
]

STORES = [
    {"store_id": "S-TLN", "store_name": "Tallinna e-pood"},
    {"store_id": "S-TRT", "store_name": "Tartu ladu"},
    {"store_id": "S-NRV", "store_name": "Narva väljastuspunkt"},
]


def stable_int(seed: str, minimum: int, maximum: int) -> int:
    """Loo sisendist alati sama täisarv etteantud vahemikus.

    Sama `seed` annab alati sama tulemuse. Nii saame teha "näiliselt juhuslikke"
    andmeid, mis on igal samal kuupäeval täpselt ühesugused.
    """
    span = maximum - minimum + 1
    seeded_text = f"{SEED_PREFIX}|{seed}"
    value = int(sha256(seeded_text.encode("utf-8")).hexdigest()[:8], 16)
    return minimum + (value % span)


def stable_fraction(seed: str) -> float:
    """Loo sisendist alati sama murdarv vahemikus 0 kuni 1.

    Seda kasutame siis, kui tahame teha mitte ainult täisarve, vaid ka
    "juhusliku" kujuga jaotusi, mis jäävad siiski sama kuupäeva jaoks
    alati samaks.
    """
    seeded_text = f"{SEED_PREFIX}|{seed}"
    value = int(sha256(seeded_text.encode("utf-8")).hexdigest()[:16], 16)
    max_value = 16**16
    # Lisame 0.5, et tulemus ei oleks kunagi täpselt 0 ega täpselt 1.
    return (value + 0.5) / max_value


def stable_gaussian(seed: str, mean: float, stddev: float) -> float:
    """Loo sisendist alati sama normaaljaotusega väärtus.

    Siin kasutame Box-Mulleri teisendust.
    Mõte on lihtne: kahest deterministlikust murdarvust saame teha arvu,
    mis käitub rohkem päris juhusliku normaaljaotusega väärtuse moodi.
    """
    u1 = stable_fraction(f"{seed}|u1")
    u2 = stable_fraction(f"{seed}|u2")
    z0 = sqrt(-2.0 * log(u1)) * cos(2.0 * pi * u2)
    return mean + (stddev * z0)


def pick_product(logical_date: date, order_no: int) -> dict:
    """Vali ühe tellimuse jaoks deterministlik toode."""
    index = stable_int(f"{logical_date.isoformat()}|product|{order_no}", 0, len(PRODUCTS) - 1)
    return PRODUCTS[index]


def pick_store(logical_date: date, order_no: int) -> dict:
    """Vali ühe tellimuse jaoks deterministlik pood."""
    index = stable_int(f"{logical_date.isoformat()}|store|{order_no}", 0, len(STORES) - 1)
    return STORES[index]


def get_order_count(logical_date: date) -> int:
    """Leia ühe päeva sündmuste arv.

    Päevane maht kõigub veidi, aga jääb enamasti umbes saja juurde.
    Kasutame siin kitsamat normaaljaotust, et päevad ei oleks kõik täpselt
    sama suurusega, kuid erinevus jääks mõistlikku vahemikku.
    """
    count = round(
        stable_gaussian(
            f"{logical_date.isoformat()}|order_count",
            AVERAGE_ORDERS_PER_DAY,
            ORDERS_PER_DAY_STDDEV,
        )
    )
    return max(ORDER_COUNT_MIN, min(ORDER_COUNT_MAX, count))


def build_orders(logical_date: date) -> list[dict]:
    """Ehita ühe päeva tellimuste loend.

    Päevane müügisündmuste arv võib veidi muutuda. Väärtused ei ole päris
    juhuslikud, vaid arvutatakse välja nii, et sama kuupäev annaks alati
    sama tulemuse.

    `source_updated_at` kirjutame Tallinna ajavööndis. Nii on aktiivse päeva
    "mis on juba nähtav?" loogikat õppijale lihtsam jälgida.
    Kellaaegade puhul kasutame normaaljaotust keskväärtusega 18:00 ja
    standardhälbega 2 tundi. See tähendab, et sündmused koonduvad rohkem
    õhtusse, nagu e-poe puhul päriselus tihti juhtub.
    """
    planned_order_count = get_order_count(logical_date)
    raw_orders = []
    # `for` loob siia loendisse ühe tellimuse korraga.
    for order_no in range(1, planned_order_count + 1):
        product = pick_product(logical_date, order_no)
        store = pick_store(logical_date, order_no)
        quantity = stable_int(f"{logical_date.isoformat()}|quantity|{order_no}", 1, 5)
        cents_step = stable_int(f"{logical_date.isoformat()}|price|{order_no}", 0, 6)
        unit_price = round(product["base_price_eur"] + (cents_step * 0.5), 2)
        # Rakendame jaotust ainult kellaajale.
        # Kui väärtus läheb üle südaöö või enne päeva algust, keerame selle
        # modulo abil sama kuupäeva sisse tagasi.
        minute_of_day = round(
            stable_gaussian(
                f"{logical_date.isoformat()}|order_time|{order_no}",
                ORDER_TIME_MEAN_MINUTES,
                ORDER_TIME_STDDEV_MINUTES,
            )
        ) % MINUTES_PER_DAY
        update_hour = minute_of_day // 60
        update_minute = minute_of_day % 60
        source_updated_at = datetime(
            logical_date.year,
            logical_date.month,
            logical_date.day,
            update_hour,
            update_minute,
            tzinfo=TALLINN_TZ,
        )
        raw_orders.append(
            {
                "order_date": logical_date.isoformat(),
                "store_id": store["store_id"],
                "product_id": product["product_id"],
                "quantity": quantity,
                "unit_price_eur": unit_price,
                "source_updated_at": source_updated_at.isoformat(),
                "_event_no": order_no,
            }
        )

    # Sorteerime sündmused kellaaja järgi, et API vastus näeks loomulikum välja.
    raw_orders.sort(
        key=lambda order: (
            order["source_updated_at"],
            order["store_id"],
            order["product_id"],
            order["_event_no"],
        )
    )

    orders = []
    for order_no, raw_order in enumerate(raw_orders, start=1):
        orders.append(
            {
                "order_id": f"ORD-{logical_date.strftime('%Y%m%d')}-{order_no:03d}",
                "order_date": raw_order["order_date"],
                "store_id": raw_order["store_id"],
                "product_id": raw_order["product_id"],
                "quantity": raw_order["quantity"],
                "unit_price_eur": raw_order["unit_price_eur"],
                "source_updated_at": raw_order["source_updated_at"],
            }
        )
    return orders


def get_business_now() -> datetime:
    """Tagasta praktikumi "praegune äriaeg".

    Praktikumis hoiame ärikuupäeva eraldi muutujas `SOURCE_BUSINESS_DATE`.
    Kellaaeg tuleb päriselt töötava masina hetkest. Nii saame simuleerida
    olukorda, kus tänane päev on veel pooleli, aga varasemad päevad on valmis.
    """
    current_local_time = datetime.now(TALLINN_TZ)
    return datetime.combine(SOURCE_BUSINESS_DATE, current_local_time.timetz())


def get_finalized_through() -> date | None:
    """Tagasta viimane päev, mida võib juba lõpetatuks pidada."""
    finalized_through = min(SOURCE_END_DATE, SOURCE_BUSINESS_DATE - timedelta(days=1))
    if finalized_through < SOURCE_START_DATE:
        return None
    return finalized_through


class RequestHandler(BaseHTTPRequestHandler):
    """Lihtne HTTP päringute töötleja.

    `BaseHTTPRequestHandler` on Pythoni standardteegi klass.
    Meie määrame siin ära, kuidas server reageerib `GET` päringutele.
    """

    server_version = "LocalShopAPI/1.0"

    def _send_json(self, status_code: int, payload: dict) -> None:
        """Saada kliendile JSON vastus koos HTTP staatusekoodiga."""
        # Kõigepealt muudame Pythoni sõnastiku JSON tekstiks ja seejärel baitideks.
        encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _send_html(self, status_code: int, html: str) -> None:
        """Saada kliendile lihtne HTML leht."""
        encoded = html.encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _build_docs_page(self) -> str:
        """Ehita brauseris loetav lühidokumentatsioon.

        Eesmärk on anda õppijale üks lihtne koht, kust on näha:

        - millised teed teenusel olemas on;
        - milliseid parameetreid tuleb kaasa anda;
        - milliste linkidega saab teenust kohe brauseris proovida.
        """
        finalized_through = get_finalized_through()
        example_date = finalized_through or SOURCE_START_DATE
        business_now = get_business_now()

        return f"""<!doctype html>
<html lang="et">
  <head>
    <meta charset="utf-8">
    <title>Local Shop Source API</title>
    <style>
      body {{
        font-family: sans-serif;
        line-height: 1.5;
        margin: 2rem auto;
        max-width: 900px;
        padding: 0 1rem 3rem;
      }}
      code {{
        background: #f3f4f6;
        padding: 0.1rem 0.3rem;
        border-radius: 4px;
      }}
      pre {{
        background: #f3f4f6;
        padding: 0.8rem 1rem;
        border-radius: 8px;
        overflow-x: auto;
      }}
      .card {{
        border: 1px solid #d1d5db;
        border-radius: 10px;
        padding: 1rem;
        margin-top: 1rem;
      }}
      ul {{
        padding-left: 1.2rem;
      }}
    </style>
  </head>
  <body>
    <h1>Local Shop Source API</h1>
    <p>See on praktikumi kohaliku allika brauseris loetav lühidokumentatsioon.</p>

    <div class="card">
      <h2>Praegune seis</h2>
      <ul>
        <li><strong>Andmeid alates:</strong> {SOURCE_START_DATE.isoformat()}</li>
        <li><strong>Andmeid kuni:</strong> {SOURCE_END_DATE.isoformat()}</li>
        <li><strong>Aktiivne äripäev:</strong> {SOURCE_BUSINESS_DATE.isoformat()}</li>
        <li><strong>Ärikell:</strong> {business_now.isoformat()}</li>
        <li><strong>Valmis päevad kuni:</strong> {finalized_through.isoformat() if finalized_through else "puudub"}</li>
        <li><strong>Sündmusi aktiivsel päeval kokku:</strong> {get_order_count(SOURCE_BUSINESS_DATE)}</li>
      </ul>
    </div>

    <div class="card">
      <h2>API teed</h2>

      <h3><code>GET /health</code></h3>
      <p>Tagastab teenuse oleku ja kuupäevakonteksti.</p>
      <ul>
        <li><code>status</code> ütleb, kas teenus töötab.</li>
        <li><code>available_from</code> ja <code>available_to</code> näitavad allika kuupäevavahemikku.</li>
        <li><code>business_date</code> näitab aktiivset äripäeva.</li>
        <li><code>finalized_through</code> näitab, millise kuupäevani peetakse andmeid lõpetatuks.</li>
      </ul>
      <p><a href="/health">Ava /health</a></p>

      <h3><code>GET /api/orders?date=YYYY-MM-DD&amp;mode=stable|fail_once</code></h3>
      <p>Tagastab ühe päeva tellimused.</p>
      <ul>
        <li><code>date</code> on loogiline kuupäev kujul <code>YYYY-MM-DD</code>.</li>
        <li><code>mode=stable</code> tagastab tavapärase vastuse.</li>
        <li><code>mode=fail_once</code> tekitab esimesel katsel ajutise vea ja sobib <code>retry</code> proovimiseks.</li>
        <li>Päevane sündmuste arv kõigub veidi, aga jääb tavaliselt umbes saja ümber.</li>
        <li>Aktiivse äripäeva puhul lisanduvad tellimused päeva jooksul vastusesse järk-järgult ja koonduvad rohkem õhtusse.</li>
      </ul>

      <p>Näited:</p>
      <ul>
        <li><a href="/api/orders?date={example_date.isoformat()}&amp;mode=stable">Valmis päeva tellimused</a></li>
        <li><a href="/api/orders?date={SOURCE_BUSINESS_DATE.isoformat()}&amp;mode=stable">Aktiivse äripäeva tellimused</a></li>
        <li><a href="/api/orders?date={example_date.isoformat()}&amp;mode=fail_once">Ajutise vea näide</a></li>
      </ul>
    </div>
  </body>
</html>
"""

    def log_message(self, fmt: str, *args) -> None:
        """Kirjuta serveri logiread terminali selgema kujuga."""
        print(f"[source-api] {self.address_string()} - {fmt % args}", flush=True)

    def do_GET(self) -> None:
        """Töötle kõik `GET` päringud.

        Selles praktikumis on üks dokumentatsioonileht ja kaks tähtsat API teed:

        - `/docs` näitab brauseris lühidokumentatsiooni;
        - `/health` ütleb, kas teenus töötab;
        - `/api/orders` tagastab ühe päeva tellimused.
        """
        parsed = urlparse(self.path)

        if parsed.path in {"/", "/docs"}:
            self._send_html(200, self._build_docs_page())
            # `return` lõpetab selle päringu töötlemise kohe siin.
            return

        if parsed.path == "/health":
            finalized_through = get_finalized_through()
            self._send_json(
                200,
                {
                    "status": "ok",
                    "service": "local-shop-source-api",
                    "available_from": SOURCE_START_DATE.isoformat(),
                    "available_to": SOURCE_END_DATE.isoformat(),
                    "business_date": SOURCE_BUSINESS_DATE.isoformat(),
                    "business_now": get_business_now().isoformat(),
                    "finalized_through": (
                        finalized_through.isoformat() if finalized_through else None
                    ),
                },
            )
            return

        if parsed.path != "/api/orders":
            self._send_json(404, {"message": "Tundmatu tee."})
            return

        query = parse_qs(parsed.query)
        # `parse_qs` tagastab väärtused nimekirjadena.
        # Seepärast võtame mõlemal väljal esimese elemendi `[0]`.
        date_value = query.get("date", [""])[0]
        mode = query.get("mode", ["stable"])[0]

        if mode not in {"stable", "fail_once"}:
            self._send_json(400, {"message": "mode peab olema stable või fail_once."})
            return

        try:
            # `fromisoformat` proovib teha tekstist kuupäevaobjekti.
            logical_date = date.fromisoformat(date_value)
        except ValueError:
            self._send_json(400, {"message": "date peab olema kujul YYYY-MM-DD."})
            return

        if logical_date > SOURCE_BUSINESS_DATE:
            self._send_json(
                404,
                {
                    "message": (
                        "Selle päeva andmed ei ole veel olemas. "
                        "Küsitud kuupäev on ärilisest tänasest päevast ees."
                    ),
                    "business_date": SOURCE_BUSINESS_DATE.isoformat(),
                },
            )
            return

        if logical_date < SOURCE_START_DATE or logical_date > SOURCE_END_DATE:
            self._send_json(
                404,
                {
                    "message": (
                        "Selle kuupäeva jaoks andmeid ei ole. Kasuta vahemikku "
                        f"{SOURCE_START_DATE.isoformat()} kuni {SOURCE_END_DATE.isoformat()}."
                    )
                },
            )
            return

        if mode == "fail_once":
            # Sama kuupäeva ja `run_id` esimene päring ebaõnnestub.
            # Järgmine sama kombinatsiooni päring õnnestub.
            run_id = self.headers.get("X-Run-Id", "direct")
            marker = FAIL_ONCE_DIR / f"{logical_date.isoformat()}__{run_id}.flag"
            if not marker.exists():
                marker.write_text("failed", encoding="utf-8")
                self._send_json(
                    503,
                    {
                        "message": (
                            "Simuleeritud ajutine tõrge. Sama päringu järgmine katse õnnestub."
                        )
                    },
                )
                return

        orders = build_orders(logical_date)
        business_now = get_business_now()
        is_final = logical_date < SOURCE_BUSINESS_DATE

        if not is_final:
            # Aktiivse päeva puhul ei näita me kohe kogu päeva tellimusi.
            # Nähtavaks saavad ainult need read, mille allika ajatempel on
            # praktikumi "praegusest ärikellaajast" varasem või sellega võrdne.
            visible_orders = []
            for order in orders:
                source_updated_at = datetime.fromisoformat(order["source_updated_at"])
                if source_updated_at <= business_now.astimezone(source_updated_at.tzinfo):
                    visible_orders.append(order)
            orders = visible_orders

        self._send_json(
            200,
            {
                "dataset": "local-shop-orders",
                "date": logical_date.isoformat(),
                "business_date": SOURCE_BUSINESS_DATE.isoformat(),
                "business_now": business_now.isoformat(),
                "is_final": is_final,
                "order_count": len(orders),
                "orders": orders,
            },
        )


def main() -> None:
    """Käivita HTTP server ja hoia seda töös kuni protsess peatatakse."""
    server = HTTPServer((HOST, PORT), RequestHandler)
    print(
        f"[source-api] Käivitus aadressil http://{HOST}:{PORT} "
        f"(andmed {SOURCE_START_DATE.isoformat()} kuni {SOURCE_END_DATE.isoformat()}, "
        f"aktiivne äripäev {SOURCE_BUSINESS_DATE.isoformat()})",
        flush=True,
    )
    # `serve_forever()` jääb päringuid ootama seni, kuni protsess peatatakse.
    server.serve_forever()


if __name__ == "__main__":
    # See plokk käivitab serveri ainult siis, kui faili jooksutatakse otse.
    main()
