# Praktikum 4: Andmetorude orkestreerimine. Airflow

## Eesmärk

Ehitada Airflow DAG, mis orkestreerib mitme allika andmete laadimist ja dbt transformatsioone. Airflow koordineerib: pärib andmed API-dest, laeb need staging-tabelitesse ja käivitab dbt, mis teisendab andmed läbi medaljonarhitektuuri (staging → intermediate → marts) üheks faktitabeliks. Praktikumi lõpuks oskad rakendada TaskFlow API-t, tagada idempotentsust ning käivitada ajalooliste andmete tagasiulatuvat töötlemist (backfill).

## Õpiväljundid

Praktikumi lõpuks osaleja:

- Loob Airflow DAG-i kasutades `@dag` ja `@task` dekoraatoreid (TaskFlow API)
- Defineerib ülesannete sõltuvusi, paralleelsust ja valikuid
- Teostab idempotentse andmete laadimise UPSERT-iga ja loogilise kuupäevaga (`data_interval_start`)
- Käivitab backfill'i ajalooliste andmete jaoks ja tõendab idempotentsust
- Orkestreerib dbt transformatsioone Airflow'st
- Oskab põhjendada orkestreerimise arhitektuurilisi valikuid

## Ülevaade

| Osa | Kestus | Sisu |
|-----|--------|------|
| Demo | ~40 min | Keskkond, @dag/@task, API-dest andmete laadimine, dbt orkestreerimine, backfill |
| Ülesanded | ~50 min | ISS ilmaandmed, valikumuster ja retry, dünaamiline kaardistamine |

---

## Eeldused

- Docker ja Docker Compose on paigaldatud
- Kogemus PostgreSQL, SQL ja Pythoniga
- Arusaam ETL etappidest ja medaljonarhitektuurist (vt praktikumid 1 ja 3)
- Eelmiste praktikumide konteinerid on peatatud (`docker compose down -v`)

## Uued mõisted

| Mõiste | Selgitus |
|--------|----------|
| **Orkestreerija** (Orchestrator) | Süsteem, mis koordineerib töövoogude käivitamist, järjekorda ja jälgimist. Orkestreerija ise andmeid ei teisenda — ta ütleb, mida, millal ja mis järjekorras käivitada. |
| **DAG** | Directed Acyclic Graph — suunatud atsükliline graaf. Airflow'is kirjeldab DAG ülesannete järjekorda ja sõltuvusi. "Atsükliline" tähendab, et ülesannete vahel ei tohi olla ringviiteid. |
| **TaskFlow API** | Airflow viis DAG-ide kirjutamiseks. Kasuta `@dag` ja `@task` dekoraatoreid Pythoni funktsioonide ümber. Funktsioonide tagastusväärtused liiguvad automaatselt järgmiste ülesannete sisenditeks. |
| **`data_interval_start`** | Airflow annab igale DAG-i käivitusele loogilise ajavahemiku. `data_interval_start` on selle algus. Kasuta seda API-päringutes — see tagab, et iga käivitus töötleb täpselt oma perioodi andmeid, mitte "praeguse hetke" andmeid. |
| **Backfill** | Möödunud perioodi andmete tagantjärele töötlemine. Airflow loob iga puuduva ajavahemiku kohta eraldi DAG-i käivituse. Toimib ainult siis, kui ülesanded kasutavad `data_interval_start` (mitte `datetime.now()`). |
| **UPSERT** | INSERT + UPDATE kombinatsioon. PostgreSQL-is `INSERT ... ON CONFLICT DO UPDATE`. Tagab idempotentsuse: sama andmerea korduv laadimine ei tekita duplikaate, vaid uuendab olemasolevat rida. |
| **Paralleelsus** | Mitu ülesannet käivituvad korraga ja järgmine samm ootab, kuni **kõik** paralleelsed ülesanded on lõppenud. Airflow'is: `[task_a, task_b] >> task_c`. |
| **Valik** (Branching) | Ülesanne valib käivitusajal, millist haru jätkata. Airflow'is kasutatakse `@task.branch` dekoraatorit — funktsioon tagastab valitud ülesande nime. |
| **Dynamic Task Mapping** | `.expand()` meetodiga dünaamiline ülesannete genereerimine. Airflow loob käivitusajal iga sisendi kohta eraldi ülesande. Kasulik, kui sisendite arv pole ette teada. |
| **Airflow Connection** | Airflow süsteemis registreeritud ühenduseparameetrid (host, port, kasutaja, parool). DAG-ides viidatakse `conn_id` kaudu, mitte hardcoded ühendusestringiga. |
| **OBT** (One Big Table) | Kõik vajalikud veerud ühes laias tabelis — dimensioonid ja mõõdikud koos. Vastupidine lähenemisele, kus faktid ja dimensioonid on eraldi (tähtskeem). OBT on analüütikas levinud, sest päringud on lihtsad (JOIN-e pole vaja) ja tööriistade ühilduvus hea. Kompromiss: tabel on lai ja redundantne (riigi nimi kordub iga ilmarida kohta). |

## Olulised viited
* **TaskFlow API ja dekoraatorid**
https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/taskflow.html
See on kaasaegse Airflow arenduse alustala, mis selgitab detailselt XCom-i automaatset haldust ja @task dekoraatorite võimalusi.

* **Dünaamiline ülesannete kaardistamine** (Dynamic Task Mapping)
https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html
See dokumentatsioon näitab, kuidas vältida tsükleid ülesannete sees ja luua paralleelseid protsesse dünaamiliselt .expand() meetodiga.

* **Kontrollvoog ja käivitusreeglid** (Control Flow & Trigger Rules)
https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#control-flow
Siit leiab elulisi näiteid ja selgitusi trigger_rule käitumise kohta, mis on kriitilise tähtsusega valikumustrite ehitamisel.

* **Andmepõhine ajastamine** (Asset-aware scheduling)
https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/asset-scheduling.html
See juhend selgitab andmepõhist ajastamist, mis võimaldab käivitada andmetorusid kohe pärast eelneva andmestiku uuenemist.

* **Airflow parimad praktikad** (Best Practices)
https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html
See leht koondab arhitektuurilised soovitused, sealhulgas raskete importide vältimise ja idempotentsuse tagamise põhimõtted.

---

## Keskkond

### Teenused

| Teenus | Konteiner | Kirjeldus |
|--------|-----------|-----------|
| Airflow API server | `praktikum-airflow-api-04` | Airflow UI ja REST API (port 8080) |
| Airflow scheduler | `praktikum-airflow-scheduler-04` | DAG-ide ajastamine ja ülesannete käivitamine |
| Airflow DAG processor | `praktikum-airflow-dagproc-04` | DAG-failide parsimine |
| Airflow metaandmebaas | `praktikum-airflow-db-04` | PostgreSQL 16, Airflow siseandmed |
| Rakenduse andmebaas | `praktikum-analytics-db-04` | PostgreSQL 16, ilma- ja riikide andmed |
| dbt | `praktikum-dbt-04` | dbt Core, SQL transformatsioonid |

### Seadistamine

1. Kopeeri `.env.example` failist `.env`:

```bash
cp .env.example .env
```

> **NB!** `.env` fail sisaldab paroole ja **ei tohi** satuda Giti repositooriumisse. Fail on lisatud `.gitignore`-sse.

2. Käivita teenused:

```bash
docker compose up -d --build
```

NB! Linuxi kasutajatel võib olla vajalik täiendavate tegevuste tegemine: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#setting-the-right-airflow-user

Esimene käivitus võtab aega (Airflow image ~1.5 GB, dbt image loomine). Airflow installib käivitusel ka `dbt-postgres` teegi, mis võtab lisaaega.

3. Kontrolli, et teenused töötavad:

```bash
docker compose ps
```

Oodatav tulemus: `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-db`, `analytics-db` ja `dbt` olekus `running` (või `Up`). `airflow-init` on olekus `Exited (0)` — see on oodatav, sest init-konteiner käivitub ainult üks kord.

4. Kontrolli, et rakenduse andmebaasi tabelid on loodud (SQL-fail `sql/create_tables.sql` käivitub automaatselt analytics-db esmakäivitusel):

```bash
docker compose exec analytics-db psql -U praktikum -c "\dt staging.*"
```

Oodatav tulemus: kolm tabelit — `countries_raw`, `weather_raw`, `iss_weather_raw`.

5. Kontrolli, et dbt ühendus töötab:

```bash
docker compose exec dbt dbt debug
```

Oodatav: "All checks passed!"

### Ühendused

| Teenus | Kasutaja | Parool | Host (konteinerist) | Port (hostist) |
|--------|----------|--------|---------------------|----------------|
| Rakenduse DB | `praktikum` | `praktikum` | `analytics-db` | 5433 |
| Airflow DB | `airflow` | `airflow` | `airflow-db` | — |
| Airflow UI | `airflow` | `airflow` | — | http://localhost:8080 |

> **Päriselus** hoitakse API-võtmeid ja andmebaasi paroole Secrets Backend'is (nt HashiCorp Vault, AWS Secrets Manager, jne) või vajadusel Airflow Connections süsteemis. Praktikumis kasutame lihtsuse mõttes keskkonnamuutujaid.

---

## Andmeallikad

Kasutame kahte avalikku API-t (kolmas lisandub harjutuses):

| Allikas | URL | Andmed | Roll pipeline'is |
|---------|-----|--------|-----------------|
| Open-Meteo Archive | `archive-api.open-meteo.com/v1/archive` | Ajaloolised ilmaandmed koordinaatide ja kuupäeva järgi | Faktiandmed (aegrida) |
| REST Countries | `restcountries.com/v3.1/alpha?codes=...` | Riigi metaandmed: nimi, pealinn, koordinaadid, rahvaarv, valuuta | Dimensiooniandmed |
| ISS positsioon *(harjutus)* | `api.open-notify.org/iss-now.json` | Rahvusvahelise kosmosejaama hetkepositsioon (laiuskraad, pikkuskraad) | Lisaallikas |

Kõik API-d on tasuta ja ei nõua API-võtit.

---

## Demo: Andmetorude orkestreerimine Airflow'ga (~40 min)

### 1. Airflow ja arhitektuur (8 min)

**Mis on Airflow?** Airflow on orkestreerija — ta ei teisenda andmeid ise, vaid juhib, mis järjekorras ja millal erinevad sammud käivituvad. Meie praktikumis:

- **Python** (`@task`) pärib API-dest andmeid ja laeb need `staging` tabelitesse
- **dbt** teisendab `staging` → `intermediate` → `marts` (medaljonarhitektuur, nagu praktikum 3)
- **Airflow** koordineerib: esmalt lae, siis teisenda, siis testi

**Arhitektuuriotsus: miks Airflow ja dbt eraldi?**

1. **Probleem.** Kuidas koordineerida andmete laadimist mitmest allikast ja transformatsioone ühes juhitavas töövoos?
2. **Variandid.** (a) Kõik ühes Python-skriptis — lihtne, aga monolitne ja raskesti jälgitav. (b) CRON + eraldi skriptid — sõltuvused pole juhitud. (c) Airflow orkestreerib, dbt transformeerib — igal tööriistal selge roll.
3. **Valik ja põhjendus.** Airflow + dbt. Airflow teab, mis järjekorras sammud käivituvad, ja käsitleb ebaõnnestumisi (retry, alerting). dbt teeb SQL-transformatsioonid andmebaasi sees, kus see on kõige efektiivsem.
4. **Kompromissid.** Rohkem infrastruktuuri (Airflow vajab oma andmebaasi, scheduler'it, API server'it). Väikese andmemahu korral võib tunduda üledimensioneeritud. Kasu tuleb, kui allikaid ja transformatsioone on rohkem.

Mine brauseris aadressile http://localhost:8080 ja logi sisse (vaikeväärtusena kasutaja: `airflow`, parool: `airflow`). See on Airflow UI, kus näed DAG-ide nimekirja, käivitusi ja logisid.

---

### 2. DAG-i struktuur ja TaskFlow API (10 min)

Ava fail `dags/weather_pipeline.py` ja uuri selle ülesehitust.

DAG-i visuaalne struktuur:

```
extract_countries → load_staging_countries ─┐
                                             ├─→ run_dbt ─→ test_dbt
extract_weather  → load_staging_weather ────┘
```

Pane tähele:

- **`@dag` dekoraator** defineerib DAG-i parameetrid (`schedule`, `start_date`, `catchup`).
- **`@task` dekoraator** muudab Pythoni funktsiooni Airflow ülesandeks.
- **Rasked impordid taski sees.** `urllib.request` ja `PostgresHook` on imporditud `@task` funktsiooni sees, mitte faili alguses. Faili ülatasemel olev kood käivitub **iga kord, kui scheduler DAG-i parsib** (vaikimisi iga 30 sekundi tagant). Raske kood ülatasemel aeglustab kogu Airflow süsteemi.
- **`CITIES` nimekiri** on faili ülatasemel — see on väike staatiline nimekiri ja ei tekita parsimisprobleeme.

> **XCom — ainult viited, mitte andmed.** TaskFlow API edastab funktsioonide tagastusväärtused automaatselt läbi XCom-i. XCom on mõeldud väikeseks metaandmeteks: failiteed, ridade arvud, olekusõnumid. Andmehulkade (list, DataFrame, JSON) edastamine XCom-i kaudu on viga — see koormab Airflow metaandmebaasi, aeglustab süsteemi ja puruneb andmemahu kasvades.
>
> Õige muster, mida see DAG kasutab:
> 1. `extract_countries` laeb andmed alla, salvestab `/tmp/countries_{ds}.json` ja tagastab **ainult failitee**.
> 2. `load_staging_countries` saab XCom-ist failitee, loeb faili ise mälusse ja laeb andmebaasi.
>
> Sama loogika kehtib `extract_weather` ja `load_staging_weather` kohta. XCom-is liigub ainult string (failitee), mitte andmed.
> Päriselus tavapärane salvestada failid nt S3 või sarnasesse objektihoidlasse.

Kontrolli, et DAG ilmub Airflow UI-s (http://localhost:8080). Kui ei ilmu, vaata logisid:

```bash
docker compose logs airflow-dag-processor | tail -20
```

---

### 3. Idempotentsus ja `data_interval_start` (7 min)

Uuri `dags/weather_pipeline.py` failis funktsioone `extract_weather` ja `load_staging_weather`.

**Kaks olulist mustrit:**

**`ds` (data_interval_start) vs `datetime.now()`:**

`extract_weather` funktsioon saab parameetrina `ds` — Airflow paneb sinna automaatselt loogilise kuupäeva stringina (nt `"2025-04-01"`). Seda kasutatakse Open-Meteo API päringus.

Kui kasutaksid selle asemel `datetime.now()`:
- Backfill ei töötaks: kõik käivitused päriksid **tänase** kuupäeva andmeid
- Korduskäivitus võiks anda erineva tulemuse → idempotentsus kaoks

**UPSERT (`ON CONFLICT DO UPDATE`):**

`load_staging_weather` kasutab SQL-i mustrit:
```sql
INSERT INTO staging.weather_raw (...) VALUES (...)
ON CONFLICT (country_code, observation_date) DO UPDATE SET ...
```

See tähendab: kui rida sama primaarvõtmega juba eksisteerib, uuenda seda. Tulemus: DAG-i korduv käivitus annab alati sama tulemuse.

---

### 4. dbt orkestreerimine ja paralleelsus (8 min)

Vaata `dags/weather_pipeline.py` faili lõppu, kus sõltuvused on defineeritud.

**Paralleelsusmuster:** `run_dbt` ülesanne käivitub alles siis, kui **mõlemad** laadimised on valmis:

```python
[countries_loaded, weather_loaded] >> dbt_run
```

See tagab, et dbt saab transformeerida ainult siis, kui kõik staging-andmed on kohal.

**dbt Airflow'st:** `run_dbt` ja `test_dbt` ülesanded kasutavad `BashOperator`-it, mis käivitab dbt CLI käsud. dbt-postgres on Airflow konteinerisse paigaldatud ja dbt projekt on volume'iga ühendatud. Pilveteenuste puhul on tavaline kutsuda välja mõni nö _serverless_ lahendus mis vastavaid dbt käske jooksutab.

Vaata dbt projekti faile `dbt_project/` kaustas:
- `models/intermediate/int_weather.sql` ja `int_countries.sql` — puhastus (view-d)
- `models/marts/obt_daily_weather.sql` — ilm + riigi info ühes laias tabelis (OBT)

Käivita DAG Airflow UI-s:

1. Mine http://localhost:8080 ja vasakul menüüs `Dags`
2. Leia `weather_pipeline` DAG
3. Lülita DAG sisse (toggle vasakul)
4. (_vajadusel_) Vajuta "Trigger DAG" (play nupp paremal)
5. Jälgi ülesannete kulgu "Graph" vaates

Kontrolli tulemust andmebaasis:

```bash
docker compose exec analytics-db psql -U praktikum -c "SELECT COUNT(*) FROM staging.countries_raw;"
```

Oodatav tulemus: `5`

```bash
docker compose exec analytics-db psql -U praktikum -c "SELECT country_name, capital, currency_code FROM staging.countries_raw ORDER BY country_name;"
```

```bash
docker compose exec analytics-db psql -U praktikum -c "SELECT city_name, observation_date, temp_max_c, temp_min_c FROM staging.weather_raw ORDER BY city_name;"
```

```bash
docker compose exec analytics-db psql -U praktikum -c "SELECT * FROM marts.obt_daily_weather ORDER BY observation_date, city_name;"
```

**Idempotentsuse kontroll:** käivita DAG Airflow UI-st uuesti. Ridade arv peab jääma samaks (5), sest UPSERT uuendab olemasolevaid ridu.

---

### 5. Backfill ja @task.virtualenv (7 min)

**Backfill** — vanade andmete tagantjärele laadimine:

```bash
docker compose exec airflow-apiserver airflow backfill create \
    --dag-id weather_pipeline \
    --from-date 2026-03-25 \
    --to-date 2026-03-31 \
    --run-backwards
```

Vaata Airflow UI-s: tekib mitu DAG Run'i (iga kuupäeva kohta üks). Iga käivitus sai oma `ds` väärtuse ja päris täpselt selle kuupäeva ilmaandmed.

Kui mõned ebaõnnestusid, siis lisame parameetrid et jookseks korraga vaid üks töövoog ja ebaõnnestunud Run'id taasjooksutataks:


```bash
docker compose exec airflow-apiserver airflow backfill create \
    --dag-id weather_pipeline \
    --from-date 2026-03-25 \
    --to-date 2026-03-31 \
    --reprocess-behavior failed \
    --max-active-runs 1 \
    --run-backwards
```

Kontrolli andmebaasis:

```bash
docker compose exec analytics-db psql -U praktikum -c "SELECT observation_date, COUNT(*) AS linnu FROM staging.weather_raw GROUP BY observation_date ORDER BY observation_date;"
```

Peaksid nägema iga kuupäeva kohta 5 rida (iga linna kohta üks).

**Idempotentsuse tõestus:** käivita sama backfill uuesti. Ridade arv peab jääma samaks — UPSERT ei lisa duplikaate.

**`@task.virtualenv` — eraldatud sõltuvused:**

Vaata `dags/weather_pipeline.py` failis funktsiooni `validate_with_duckdb`. See kasutab `@task.virtualenv(requirements=["duckdb==1.3.0"])` dekoraatorit, mis loob ülesande jaoks ajutise virtuaalkeskkonna. DuckDB pole Airflow baasimage'is, aga `@task.virtualenv` installib selle automaatselt.

**Arhitektuuriotsus: @task.virtualenv vs Docker image'i laiendamine**

1. **Probleem.** Ülesanne vajab teeki, mida Airflow baasimage'is pole.
2. **Variandid.** (a) Lisa teek Docker image'i — kiire käivitus, aga image ehitamine aeglasem. (b) `@task.virtualenv` — installib käivitusajal.
3. **Valik ja põhjendus.** `@task.virtualenv` on kasulik harva käivituvate ülesannete jaoks — pole vaja image'i uuesti ehitada.
4. **Kompromissid.** `@task.virtualenv` on aeglasem (installimine iga käivitus) ja nõuab internetiühendust. Sageli käivituvate ülesannete jaoks on parem lisada teek image'i.

---

## Ülesanne 1: ISS ilmaandmed (~20 min)

### Taust

Demo-s ehitasime andmetoru, mis laeb Euroopa pealinnade ilmaandmeid. Nüüd soovime näha, milline on ilm rahvusvahelise kosmosejaama (ISS) juures. ISS tiirleb Maa ümber ~90 minutiga ja selle positsioon muutub pidevalt — iga DAG-i käivituse ajal on ISS erinevas kohas.

### Ülesanne

Lisa `dags/weather_pipeline.py` faili kolm uut ülesannet:

1. `extract_iss_position` — pärib ISS hetkeasukoha
2. `extract_iss_weather` — kasutab ISS koordinaate Open-Meteo API-st hetkeilma pärimiseks
3. `load_staging_iss_weather` — laeb tulemuse `staging.iss_weather_raw` tabelisse

### Nõuded

- ISS API endpoint: `http://api.open-notify.org/iss-now.json`
- Vastuse struktuur:
  ```json
  {"message": "success", "iss_position": {"latitude": "-19.8482", "longitude": "113.2330"}, "timestamp": 1775392616}
  ```
- Hetkeilm Open-Meteo API-st: `https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current=temperature_2m,wind_speed_10m,weather_code`
- `load_staging_iss_weather` kasutab UPSERT-i (primaarvõti: `observation_time`)
- `run_dbt` ootab ka `iss_loaded` valmimist (paralleelsusmuster):
  ```python
  [countries_loaded, weather_loaded, iss_loaded] >> dbt_run
  ```

### Vihjed

- ISS API ei vaja kuupäevaparameetrit — tagastab alati hetkepositsiooni
- Kasuta sama mustrit nagu demo-s: `extract_iss_position` salvestab positsiooni faili (`/tmp/iss_position_{ds}.json`) ja tagastab failitee XCom-i kaudu
- `extract_iss_weather` võtab failitee sisendiks, loeb positsiooni failist ja salvestab ilmaandmed eraldi faili; tagastab selle failitee
- `load_staging_iss_weather` võtab failitee sisendiks ja laeb andmed andmebaasi
- Hetkeilma vastuses on andmed `current` võtme all: `temperature_2m`, `wind_speed_10m`, `weather_code`
- Staging tabel `iss_weather_raw` on juba loodud (vt `sql/create_tables.sql`)

### Sammud

1. Lisa kolm uut `@task` funktsiooni DAG-i
2. Ühenda sõltuvused: ISS haru paralleelselt ilmaharu kõrval
3. Trigger DAG Airflow UI-s
4. Kontrolli tulemust:
   ```bash
   docker compose exec analytics-db psql -U praktikum -c "SELECT * FROM staging.iss_weather_raw ORDER BY observation_time DESC LIMIT 5;"
   ```

### Aruteluks

- ISS API tagastab ainult hetkepositsiooni. Kuidas see mõjutab backfill'i? Kas saaksime tagantjärele töödelda ISS andmeid?
- Mille poolest erineb see ülesanne `extract_weather` mustrist, kus kasutame `data_interval_start`?
- Kuidas käsitleksid olukorda, kus ISS API on maas, aga ülejäänud pipeline peaks jätkama?

---

## Ülesanne 2: Valikumuster ja retry (~15 min)

### Taust

Riikide andmed muutuvad harva (rahvaarv, pealinn, valuuta). Pole mõtet neid iga DAG-i käivitusel uuesti laadida. Airflow `@task.branch` dekoraator võimaldab käivitusajal valida, millist haru jätkata — see on **valikumuster** (branching).

Lisaks tahame, et API-päringud oleksid vastupidavad ajutistele tõrgetele. Airflow retry-mehhanism kordab ebaõnnestunud ülesannet automaatselt.

### Ülesanne

1. Lisa `@task.branch` funktsioon `choose_countries_path`, mis otsustab, kas riikide laadimine on vajalik
2. Lisa `extract_weather` ülesandele: `retries=3`, `retry_delay=timedelta(minutes=1)`
3. Testi retry-käitumist

### Nõuded

- `choose_countries_path` kontrollib, kas `staging.countries_raw` tabel on tühi
  - Tühi → tagasta `"extract_countries"` (laadi riikide andmed)
  - Ei ole tühi → tagasta `"run_dbt"` (jäta riikide laadimine vahele)
- `@task.branch` tagastab valitud ülesande `task_id` stringina
- Lisa `trigger_rule="none_failed_min_one_success"` `run_dbt` ülesandele, et see käivituks ka siis, kui mõni haru jäeti vahele

### Vihjed

```python
@task.branch
def choose_countries_path():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    hook = PostgresHook(postgres_conn_id="analytics_db")
    count = hook.get_first("SELECT COUNT(*) FROM staging.countries_raw")[0]
    if count == 0:
        return "extract_countries"
    return "run_dbt"
```

Retry dekoraatoris:
```python
@task(retries=3, retry_delay=timedelta(minutes=1))
def extract_weather(ds=None):
    ...
```

Retry testimiseks muuda ajutiselt API URL valeks ja vaata Airflow UI task logis, kuidas Airflow ülesannet kordab.

### Aruteluks

- Millal peaks pipeline andma hoiatuse (retry) ja millal kohe ebaõnnestuma (fail)?
- Mis vahe on `@task.branch` ja `@task.short_circuit` vahel?
- Kuidas käsitleksid olukorda, kus riikide andmed on küll olemas, aga aegunud (nt üle 30 päeva vanad)?

---

## Ülesanne 3: Dynamic Task Mapping (boonus, ~15 min)

### Taust

Praegu pärib `extract_weather` kõigi linnade ilmaandmed ühes tsüklis. Kui linnu on palju või API on aeglane, võib ülesanne kesta kaua. Airflow Dynamic Task Mapping (`.expand()`) võimaldab luua iga linna kohta eraldi ülesande, mis käivituvad paralleelselt.

### Ülesanne

Muuda `extract_weather` dünaamiliseks: iga linna kohta eraldi ülesanne.

### Nõuded

1. Lisa `@task` funktsioon `get_cities`, mis tagastab linnade nimekirja
2. Muuda `extract_weather` nii, et see võtab vastu ühe linna (mitte kogu nimekirja)
3. Kasuta `.expand()` meetodit dünaamiliseks kaardistamiseks
4. `load_staging_weather` peab koguma kõigi linnade tulemused kokku

### Vihjed

```python
@task
def get_cities() -> list[dict]:
    return CITIES

@task
def extract_single_city_weather(city: dict, ds=None) -> dict:
    """Pärib ilmaandmed ühele linnale."""
    ...

# Dünaamiline kaardistamine — iga linna kohta eraldi task
cities = get_cities()
weather_data = extract_single_city_weather.expand(city=cities)
weather_loaded = load_staging_weather(weather_data)
```

Vaata Airflow UI "Graph" vaatest, kuidas Airflow loob iga linna kohta eraldi ülesande.

### Aruteluks

- Millal on dünaamiline kaardistamine parem kui tsükkel ühe ülesande sees?
- Mis juhtub, kui ühe linna API-päring ebaõnnestub? Kas teised linnad peatuvad?
- Kuidas mõjutab `.expand()` paralleelsete ülesannete arvu? Mis on `max_active_tis_per_dag` seadistus?

---

## Levinud vead ja tõrkeotsing

### DAG ei ilmu Airflow UI-s

**Sümptom:** DAG-i nime ei näe Airflow UI DAG-ide nimekirjas.

**Diagnostika:**
```bash
docker compose logs airflow-dag-processor | tail -30
```

**Lahendus:** Tavaliselt on tegemist süntaksiveaga DAG-failis. Paranda viga ja oota ~30 sekundit.

### Import error mooduli tasemel

**Sümptom:** DAG processor logis näed `ModuleNotFoundError`.

**Diagnostika:** Vaata, kas impordid on taski sees (mitte faili alguses).

**Lahendus:** Liiguta raske import `@task` funktsiooni sisse. Faili ülatasemel tohivad olla ainult `from airflow.sdk import ...` ja standardteegi impordid.

### Connection refused analytics-db vastu

**Sümptom:** `load_staging_*` ülesanne ebaõnnestub veaga `connection refused`.

**Diagnostika:**
```bash
docker compose ps analytics-db
docker compose exec analytics-db pg_isready -U praktikum
```

**Lahendus:** Oota, kuni analytics-db healthcheck läbib. Vaata logisid: `docker compose logs analytics-db`.

### Backfill ei loo käivitusi

**Sümptom:** `airflow backfill create` ei tekita uusi DAG Run'e.

**Lahendus:** Kontrolli, et DAG-i `start_date` on varasem kui backfill'i `--from-date`. Samuti kontrolli, et archive API-s on andmed valitud kuupäevade kohta olemas (viivitus ~5–7 päeva).

### dbt build ebaõnnestub

**Sümptom:** `run_dbt` ülesanne ebaõnnestub.

**Diagnostika:**
```bash
docker compose exec dbt dbt debug
docker compose exec dbt dbt build 2>&1
```

**Lahendus:** Kontrolli, et `profiles.yml` failis on õige host (`analytics-db`) ja et staging tabelid on loodud.

### Pordikonflikt

**Sümptom:** `docker compose up` annab vea `port 8080 already in use`.

**Lahendus:** Peata eelmiste praktikumide konteinerid:
```bash
cd ../03-andmete-integreerimine/edasijoudnud/
docker compose down
```

---

## Keskkonna sulgemine

```bash
# Peata konteinerid
docker compose down

# Peata konteinerid JA kustuta andmed (andmebaasi sisu kaob)
docker compose down -v
```
