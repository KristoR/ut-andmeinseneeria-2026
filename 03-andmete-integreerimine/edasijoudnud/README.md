# Praktikum 3: Andmete integreerimine — dbt ja medaljonarhitektuur (edasijõudnud)

## Eesmärk

Ehitada andmete integratsiooni torujuhe (pipeline), kus Python-skript laeb andmed kahest API-st PostgreSQL-i ning dbt teisendab need läbi kolme kihi (staging → intermediate → marts). Praktikumi lõpuks oskad rakendada inkrementaalset laadimist, kirjeldada idempotentsust ning jälgida andmete ajaloolist muutumist SCD Type 2 snapshotiga.

## Õpiväljundid

Praktikumi lõpuks osaleja:

- Suudab teha inkrementaalse andmete laadimise ja kirjeldada idempotentsust
- Ehitab ETL-i nii, et see on logitud ja vead on hallatavad
- Tunneb medaljonarhitektuuri (staging → intermediate → marts) põhimõtteid
- Mõistab dbt materialiseerimisi (view, table, incremental) ja oskab neid valida
- Oskab jälgida andmete ajaloolist muutumist dbt snapshotiga (SCD Type 2)

## Ülevaade

| Osa | Kestus | Sisu |
|-----|--------|------|
| Demo | ~40 min | Andmete laadimine API-st, medaljonarhitektuur, dbt materialiseerimine, SCD2, testid |
| Ülesanded | ~50 min | Inkrementaalne mudel, testid ja andmekvaliteet, aktiivsusraport |

---

## Eeldused

- Docker ja Docker Compose on paigaldatud
- Kogemus PostgreSQL, SQL ja Pythoniga
- Arusaam ETL etappidest (vt praktikum 1)
- Eelmiste praktikumide konteinerid on peatatud (`docker compose down`)

## Uued mõisted

| Mõiste | Selgitus |
|--------|----------|
| **Medaljonarhitektuur** | Andmete organiseerimise muster kolme kihiga: bronze (toorandmed, "as-is"), silver (puhastatud, normaliseeritud), gold (ärikasutuseks valmis mudelid, aggregatsioonid). Iga kiht lisab kvaliteeti ja struktuuri. |
| **dbt** | Data Build Tool — SQL-põhine transformatsioonitööriist. Mudelid on SELECT-laused, mille dbt materialiseerib tabeliteks, vaadeteks vms. |
| **Materialiseerimine** | Kuidas dbt mudeli tulemuse salvestab: `view` (vaade, arvutatakse iga päringu ajal), `table` (tabel, ehitatakse iga kord uuesti), `incremental` (tabel, töötleb ainult uued/muutunud read, mitte kogu tabelit). |
| **Inkrementaalne laadimine** | Laetakse ainult andmed, mis on lisandunud pärast viimast käivitust. Vähendab aega ja ressursse. |
| **Idempotentsus** | Sama operatsiooni korduv käivitamine annab alati sama tulemuse. Oluline, et töövoogu saaks ohutult uuesti käivitada. |
| **SCD Type 2** | Slowly Changing Dimension Type 2 — ajalooliste muudatuste jälgimine. Iga muudatuse korral salvestatakse nii vana kui uus versioon koos kehtivusperioodiga. |
| **Snapshot** | dbt-s: mehhanism SCD Type 2 rakendamiseks. Käivitamisel võrdleb andmeid eelmise seisuga ja loob uued versioonid muutunud ridadele. |
| **source()** | dbt funktsioon viitamiseks lähteandmetabelitele (nt staging skeema), mida dbt ise ei loo. Võimaldab ka andmete värskuse kontrolli (source freshness). |
| **ref()** | dbt funktsioon viitamiseks teistele dbt mudelitele. Loob automaatselt sõltuvusgraafi (dependency graph, vt ka [dbt DAG](https://www.getdbt.com/blog/dag-use-cases-and-best-practices)). |

---

## Keskkond

### Teenused

| Teenus | Konteiner | Kirjeldus |
|--------|-----------|-----------|
| PostgreSQL | `praktikum-db-03` | Andmebaas (pgduckdb) |
| Python | `praktikum-python-03` | Python 3.13, API-st andmete laadimine |
| dbt | `praktikum-dbt-03` | dbt Core, SQL transformatsioonid |

### Seadistamine

1. Kopeeri `.env.example` failist `.env`:

```bash
cp .env.example .env
```

2. Vajadusel muuda `.env` failis kasutajanimed ja paroolid.

> **NB!** `.env` fail sisaldab paroole ja **ei tohi** satuda Giti repositooriumisse. Fail on lisatud `.gitignore`-sse. Repositooriumis on ainult `.env.example` näidisväärtustega.

3. Käivita teenused:

```bash
docker compose up -d --build
```

4. Kontrolli, et kõik teenused töötavad:

```bash
docker compose ps
```

Oodatav tulemus: kolm konteinerit olekus `running` (või `Up`).

### Ühendused

| Teenus | Kasutaja | Parool | Port |
|--------|----------|--------|------|
| PostgreSQL | `praktikum` | `praktikum` | 5432 |

Andmebaasiga saab ühenduda ka otse hostist: `psql -h localhost -U praktikum`

### Esimene test

```bash
docker compose exec dbt dbt debug
```

Kui näed "All checks passed!", on keskkond valmis.

---

## Andmeallikad

Kasutame kahte avalikku API-t:

| Allikas | URL | Andmed | Eesmärk praktikumis |
|---------|-----|--------|---------------------|
| randomuser.me | `randomuser.me/api/` | Kasutajaprofiilid: nimi, aadress, email, registreerimisaeg | SCD2 demonstratsioon |
| JSONPlaceholder | `jsonplaceholder.typicode.com/posts` | Postitused: pealkiri, sisu, kasutaja ID (1-10) | Inkrementaalse laadimise demonstratsioon |

**Ühendamine:** randomuser.me tagastab parameetriga `seed=praktikum` alati samad 10 kasutajat. Python-skript omistab neile numbrilised ID-d 1-10. JSONPlaceholder postitustel on juba `userId` 1-10. Nii saame kaks allikat omavahel ühendada.

> Päriselus tuleb erinevatest allikatest pärit andmete ID-sid sageli ühildada. Siin teeme seda teadlikult lihtsana, et keskenduda dbt-le.

---

## Demo: Andmete integreerimine dbt-ga (~40 min)

### 1. Andmete laadimine API-st (5 min)

Kõigepealt laeme toorandmed API-st PostgreSQL-i `staging` skeemasse. Seda teeb Python-skript `ingest.py`.

Vaata skripti sisu (`ingest.py`). Pööra tähelepanu:
- Keskkonnamuutujad andmebaasiühenduseks (mitte hardcoded paroolid)
- `ON CONFLICT ... DO UPDATE` — tagab idempotentsuse (korduv käivitamine ei tekita duplikaate). NB - see toimib nii PostgreSQL puhul. Teistel andmebaasimootoritel võib olla teistsugune süntaks ja funktsionaalsus.
- `staging.etl_log` tabel — logib iga laadimise algus- ja lõppaja, ridade arvu, vead

```bash
# Laadi kasutajad (10 tk, alati samad tänu seed parameetrile)
docker compose exec python python ingest.py users

# Laadi postitused — esimene partii (id 1-50)
docker compose exec python python ingest.py posts --batch 1
```

Kontrolli tulemust:

```bash
docker compose exec db psql -U praktikum -c "SELECT COUNT(*) FROM staging.users;"
```

Oodatav tulemus: `10`

```bash
docker compose exec db psql -U praktikum -c "SELECT first_name, last_name, city, email FROM staging.users ORDER BY id LIMIT 5;"
```

```bash
docker compose exec db psql -U praktikum -c "SELECT COUNT(*) FROM staging.posts;"
```

Oodatav tulemus: `50`

```bash
docker compose exec db psql -U praktikum -c "SELECT * FROM staging.etl_log ORDER BY id;"
```

Näed kaht rida: üks kasutajate ja teine postituste laadimiseks. Mõlemal `status = 'success'`.

**Idempotentsuse kontroll:** käivita `docker compose exec python python ingest.py users` uuesti. Ridade arv jääb samaks (10), sest `ON CONFLICT` uuendab olemasolevaid ridu.

---

### 2. Medaljonarhitektuur (5 min)

Andmed liiguvad läbi kolme kihi:

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│staging (API) │ ──> │ intermediate │ ──> │    marts     │
│   pronks     │     │    hõbe      │     │    kuld      │
│              │     │              │     │              │
│ Toorandmed   │     │ Puhastatud,  │     │ Äriloogika,  │
│ nagu API     │     │ tüübitud,    │     │ dimensioonid │
│ need andis   │     │ valideeritud │     │ ja faktid    │
└──────────────┘     └──────────────┘     └──────────────┘
     Python               dbt                  dbt
```

| Kiht | Skeema | Kes haldab | Mida teeb |
|------|--------|------------|-----------|
| Staging (pronks) | `staging` | Python `ingest.py` | Salvestab API vastuse sellisena nagu see tuli |
| Intermediate (hõbe) | `intermediate` | dbt (view) | Puhastab: TRIM, LOWER, INITCAP. Filtreerib tühjad read. |
| Marts (kuld) | `marts` | dbt (table/incremental) | Äriloogika: dimensioonitabelid, faktitabelid, kokkuvõtted |

**Arhitektuuriotsus:**

1. **Probleem.** Kuidas organiseerida andmeid nii, et toorandmed säiliksid, aga äripäringud oleksid kiired ja puhtad?
2. **Variandid.** (a) Kõik ühes skeemas — lihtne, aga segane. (b) Raw + final — puudub puhastuskiht. (c) Medaljonarhitektuur (staging → intermediate → marts) — kolm selget kihti.
3. **Valik ja põhjendus.** Medaljonarhitektuur, sest iga kiht täidab selget rolli. Staging säilitab toorandmed (auditi jaoks), intermediate puhastab, marts pakub ärivaadet.
4. **Kompromissid.** Rohkem skeemasid hallata. Väikese andmemahu korral on lisakiht üleliigne. Suuremates süsteemides on kasu selge. Võib isegi vaja minna rohkem skeemasid (vt nt: https://www.ssp.sh/brain/medallion-architecture/#historical-context).

---

### 3. dbt mudelid ja materialiseerimine (10 min)

Vaata projekti struktuuri:

```bash
docker compose exec dbt ls -la models/intermediate/
docker compose exec dbt ls -la models/marts/
docker compose exec dbt cat dbt_project.yml
```

Pane tähele `dbt_project.yml` failis:
- `intermediate` kihi materialiseerimine on `view` — ei loo tabelit, vaid vaate
- `marts` kihi materialiseerimine on `table` — loob füüsilise tabeli

**Miks intermediate on view?** Intermediate kiht teeb ainult puhastust (TRIM, LOWER). Pole mõtet seda eraldi tabelina salvestada — vaade arvutab tulemuse igal päringul otse staging andmetest. Kui intermediate loogika muutub, pole vaja midagi uuesti ehitada.

**Miks marts on table?** Marts kihi mudelid teevad JOIN-e ja agregatsioone. Tabelina salvestamine tagab kiired SELECT päringud.

Tavapärane on läheneda view -> table -> incremental põhimõttel:
* View: lihtne ja kiire luua, aga SELECT päringud on aeglased
* Table: lihtne luua, SELECT päringud kiired, aga loomine (CREATE) on aeglane
* Incremental: keeruline loogika, aga CREATE/INSERT ja SELECT päringud on kiired

Käivita mudelid:

```bash
# Intermediate mudelid (loob vaated)
docker compose exec dbt dbt run --select intermediate
```

Vaata, et intermediate skeemasse tekkisid vaated:

```bash
docker compose exec db psql -U praktikum -c "\dv intermediate.*"
```

Oodatav tulemus: `int_users` ja `int_posts` vaated.

```bash
# Marts mudelid (loob tabelid)
docker compose exec dbt dbt run --select marts
```

```bash
docker compose exec db psql -U praktikum -c "\dt marts.*"
```

Oodatav tulemus: `dim_users`, `fct_posts`, `user_post_summary` tabelid.

Vaata andmeid:

```bash
docker compose exec db psql -U praktikum -c "SELECT user_key, full_name, city, country FROM marts.dim_users ORDER BY full_name LIMIT 5;"
docker compose exec db psql -U praktikum -c "SELECT post_id, author_name, title, body_length FROM marts.fct_posts ORDER BY post_id LIMIT 5;"
```

**`ref()` ja sõltuvusgraaf:**

dbt mudelites kasutatakse `{{ ref('int_users') }}` viitamiseks teistele mudelitele. See loob automaatselt sõltuvusgraafi — dbt teab, millises järjekorras mudeleid käivitada.

```bash
docker compose exec dbt dbt ls --resource-type model
```

Näed kõiki mudeleid ja nende kihte. Käivitamisel teab dbt sõltuvusgraafi põhjal, et intermediate mudelid tuleb enne marts mudeleid käivitada.

```bash
docker compose exec dbt dbt docs generate
```

```bash
docker compose exec dbt dbt docs serve --host 0.0.0.0
```

Mine brauseris http://localhost:18080/. Siit on näha dbt projekti infot ning sõltuvusgraaf (pärinevus).

---

### 4. Inkrementaalne mudel (5 min)

Vaata faili `models/marts/user_post_summary.sql`:

```sql
{{ config(
    materialized='incremental',
    unique_key='user_key'
) }}

SELECT
    u.uuid              AS user_key,
    u.first_name,
    u.last_name,
    COUNT(p.post_id)    AS total_posts,
    AVG(LENGTH(p.body)) AS avg_post_length,
    MAX(p.loaded_at)    AS last_post_loaded_at
FROM {{ ref('int_users') }} u
LEFT JOIN {{ ref('int_posts') }} p
    ON u.user_id = p.user_id
{% if is_incremental() %}
WHERE p.loaded_at > (SELECT MAX(last_post_loaded_at) FROM {{ this }})
{% endif %}
GROUP BY u.uuid, u.first_name, u.last_name
```

Võtmepunktid:
- `materialized='incremental'` — esimesel korral CREATE TABLE, edaspidi INSERT/UPDATE
- `unique_key='user_key'` — kui kasutaja juba eksisteerib, uuendatakse rida (mitte ei lisata uut)
- `is_incremental()` — tagastab `true` ainult siis, kui tabel juba eksisteerib. Esimesel käivitusel laetakse kõik andmed.

Kontrolli praegust seisu:

```bash
docker compose exec db psql -U praktikum -c "SELECT user_key, first_name, total_posts, avg_post_length FROM marts.user_post_summary ORDER BY total_posts DESC;"
```

Iga kasutaja peaks nägema ~5 postitust (50 postitust / 10 kasutajat).

Nüüd laadi teine partii postitusi:

```bash
docker compose exec python python ingest.py posts --batch 2
```

Kontrolli, et staging tabelis on nüüd 100 postitust:

```bash
docker compose exec db psql -U praktikum -c "SELECT COUNT(*) FROM staging.posts;"
```

Käivita inkrementaalne mudel uuesti:

```bash
docker compose exec dbt dbt run --select user_post_summary
```

Jälgi dbt väljundit terminalis: tulemuse real näed `INSERT` (mitte `CREATE TABLE`). See tähendab, et dbt lisas ainult uued andmed, mitte ei ehitanud tabelit algusest.

Kontrolli tulemust:

```bash
docker compose exec db psql -U praktikum -c "SELECT user_key, first_name, total_posts, avg_post_length FROM marts.user_post_summary ORDER BY total_posts DESC;"
```

Iga kasutaja peaks nägema ~10 postitust (100 / 10).

**`--full-refresh`:** sunnib dbt ehitama tabeli algusest peale:

```bash
docker compose exec dbt dbt run --full-refresh --select user_post_summary
```

Tulemus on sama (idempotentsus!), aga dbt teeb DROP + CREATE, mitte INSERT.

---

### 5. SCD Type 2 — snapshot (10 min)

Snapshot jälgib andmete muutumist ajas. Kui kasutaja aadress muutub, salvestab dbt nii vana kui uue versiooni.

Vaata faili `snapshots/snap_users.sql`:

```sql
{% snapshot snap_users %}
{{ config(
    unique_key='uuid',
    strategy='check',
    check_cols=['city', 'street', 'state', 'country'],
    target_schema='snapshots',
    dbt_valid_to_current="to_timestamp('9999-12-31', 'YYYY-MM-DD')"
) }}

SELECT
    uuid, first_name, last_name, email,
    city, street, state, country, registered_date
FROM {{ source('staging', 'users') }}
{% endsnapshot %}
```

Pane tähele:
- `strategy='check'` — dbt võrdleb `check_cols` veerge eelmise seisuga
- `check_cols` sisaldab aadressiveerge — just neid me hiljem muudame
- `target_schema='snapshots'` — snapshot tabel luuakse eraldi skeemasse (mitte marts-i)
- Snapshot loeb otse `staging.users` tabelist, mitte intermediate-ist

**Miks eraldi skeema, mitte marts?** Snapshot-tabelid koguvad ajalugu *käivituste vahel* ja neid ei tohi kunagi kustutada — ajalugu pole taastatav. Marts mudeleid saab alati `--full-refresh`-iga uuesti ehitada. Eraldi skeema kaitseb kogematu kustutamise eest. Marts kihis saab snapshotist lugeda `{{ ref('snap_users') }}` kaudu (vt ülesanne 3).

**Esimene snapshot:**

```bash
docker compose exec dbt dbt snapshot
```

Kontrolli tulemust:

```bash
docker compose exec db psql -U praktikum -c "
SELECT uuid, first_name, city, dbt_valid_from, dbt_valid_to
FROM snapshots.snap_users
ORDER BY last_name;
"
```

Kõigil ridadel on `dbt_valid_to = 9999-12-31` — see tähendab, et kõik versioonid on praegu kehtivad.

**Simuleerime allikaandmete muutumist:**

Päriselus muutuvad andmed allsüsteemis. Siin simuleerime seda käsitsi UPDATE-ga:

```bash
docker compose exec db psql -U praktikum -c "
SELECT id, first_name, last_name, city, street FROM staging.users ORDER BY id LIMIT 5;
"
```

Pane tähele kasutajate 1 ja 3 praegused linnad. Nüüd muudame need:

```bash
docker compose exec db psql -U praktikum -c "
UPDATE staging.users SET city = 'Tallinn', street = 'Viru 1' WHERE id = 1;
UPDATE staging.users SET city = 'Berlin', street = 'Unter den Linden 5' WHERE id = 3;
"
```

**Teine snapshot:**

```bash
docker compose exec dbt dbt snapshot
```

Vaata tulemust:

```bash
docker compose exec db psql -U praktikum -c "
SELECT uuid, first_name, last_name, city, street, dbt_valid_from, dbt_valid_to
FROM snapshots.snap_users
ORDER BY uuid, dbt_valid_from;
"
```

Muudetud kasutajatel on nüüd **kaks rida**:
- Vana rida: `dbt_valid_to` on täidetud tegeliku kuupäevaga (pole enam kehtiv)
- Uus rida: `dbt_valid_to = 9999-12-31` (praegu kehtiv)

Mitu versiooni on igal kasutajal?

```bash
docker compose exec db psql -U praktikum -c "
SELECT first_name, last_name, COUNT(*) AS versioonide_arv
FROM snapshots.snap_users
GROUP BY first_name, last_name
ORDER BY versioonide_arv DESC;
"
```

Kahe kasutaja juures peaks olema 2 versiooni, ülejäänutel 1.

**Miks see kasulik on?** Päriselus muutuvad klientide aadressid, toodete hinnad, töötajate ametinimetused. SCD Type 2 võimaldab analüüsida andmeid nii praeguse kui ajaloolise seisuga — näiteks: "Kui palju müüki toimus, kui klient elas veel Tartus?"

---

### 6. Testid (5 min)

dbt-s on kahte liiki teste:

**Schema testid** — defineeritud YAML-failis (`schema.yml`). Sisseehitatud testid: `unique`, `not_null`, `accepted_values`, `relationships`.

Vaata `models/intermediate/schema.yml` ja `models/marts/schema.yml`.

**Kohandatud testid** (singular tests) — eraldi SQL-failid kaustas `tests/`. Test läbib, kui päring tagastab **0 rida**.

Vaata `tests/assert_valid_email.sql`:

```sql
SELECT *
FROM {{ ref('int_users') }}
WHERE email NOT LIKE '%@%'
```

Kui päring tagastab ridu, tähendab see vigaseid meiliaadrsse → test ebaõnnestub.

Käivita kõik testid:

```bash
docker compose exec dbt dbt test
```

Vaata tulemust: iga test näitab `Pass` või `Fail`. Kõik peaksid läbima.

```bash
# Vaata, millised testid on defineeritud
docker compose exec dbt dbt ls --resource-type test
```

---

## Ülesanne 1: Inkrementaalne postituste päevakokkuvõte (~20 min)

### Taust

Demo-s nägid `user_post_summary` mudelit, mis koondab postituste statistika kasutaja kohta. Nüüd ehitad sarnase mudeli, aga detailsema granuleeritusega: **kasutaja + päev**.

### Ülesanne

Loo fail `models/marts/post_activity_daily.sql`, mis arvutab iga kasutaja postituste statistika päevakaupa.

### Nõuded

- **Materialiseerimine:** `incremental`
- **Unikaalne liitvõti (unique composite key):** `['user_key', 'load_date']` — üks rida iga kasutaja + päev kombinatsiooni kohta
- **Veerud:**
  - `user_key` — kasutaja UUID
  - `load_date` — postituste laadimise kuupäev (DATE, mitte TIMESTAMP)
  - `posts_count` — postituste arv
  - `avg_body_length` — keskmine postituse pikkus (tähemärkides)
  - `total_body_chars` — postituste tähemärkide koguarv
- **Inkrementaalne loogika:** `is_incremental()` blokk filtreerib `load_date` järgi

### Vihjed

- `loaded_at::DATE` teisendab ajatempli kuupäevaks
- Unikaalne liitvõti: `unique_key=['user_key', 'load_date']`
- Kasuta `int_posts` ja `int_users` mudeleid (`ref()` kaudu)
- `ROUND(AVG(LENGTH(p.body)))` ümardab keskmise täisarvuks

### Sammud

1. Loo fail `models/marts/post_activity_daily.sql`
2. Käivita mudel:
   ```bash
   docker compose exec dbt dbt run --select post_activity_daily
   ```
3. Kontrolli tulemust:
   ```bash
   docker compose exec db psql -U praktikum -c "SELECT * FROM marts.post_activity_daily ORDER BY user_key, load_date LIMIT 10;"
   docker compose exec db psql -U praktikum -c "SELECT COUNT(*) FROM marts.post_activity_daily;"
   ```
4. Käivita uuesti ja kontrolli idempotentsust — ridade arv peab jääma samaks:
   ```bash
   docker compose exec dbt dbt run --select post_activity_daily
   docker compose exec db psql -U praktikum -c "SELECT COUNT(*) FROM marts.post_activity_daily;"
   ```
5. Proovi `--full-refresh` ja võrdle tulemust:
   ```bash
   docker compose exec dbt dbt run --full-refresh --select post_activity_daily
   ```

### Aruteluks

- Mis juhtub, kui `loaded_at` on alati `NOW()`? Vihje: iga käivitamine tekitab "uued" read.
- Kuidas käsitleksid hilinevaid andmeid (andmed, mis saabuvad hiljem, aga kuuluvad varasemasse perioodi)?
- Mis on liitvõtme ja ühe veeru põhise võtme unikaalsuse (unique_key) erinevus?

### Näidislahendus

`ylesanne1_post_activity_daily.sql`

---

## Ülesanne 2: dbt testid ja andmekvaliteet (~15 min)

### Taust

Andmekvaliteet ei ole enesestmõistetav. Allikaandmetes võib olla puuduvaid väärtusi, vigaseid viiteid ja ootamatuid tüüpe. dbt testid aitavad neid probleeme automaatselt tuvastada.

### Osa A: Schema testid

Lisa järgmised testid `models/marts/schema.yml` faili:

1. **`fct_posts.user_key`** — lisa `relationships` test, mis kontrollib, et iga `user_key` väärtus eksisteerib ka `dim_users` tabelis:
   ```yaml
   - relationships:
       to: ref('dim_users')
       field: user_key
   ```

2. **`fct_posts.body_length`** — lisa `not_null` test

3. **`dim_users.email`** — lisa `not_null` test

### Osa B: Kohandatud test

Loo fail `tests/assert_no_orphan_posts.sql`, mis leiab postitused, mille kasutajat `dim_users` tabelis ei eksisteeri:

- Kasuta `LEFT JOIN` mustrit `fct_posts` ja `dim_users` vahel
- Filtreeri `WHERE u.user_key IS NULL`
- Test läbib, kui päring tagastab 0 rida

### Osa C: Käivita ja interpreteeri

```bash
docker compose exec dbt dbt test
```

Vaata tulemust. Kui mõni test ebaõnnestub, loe veateadet ja mõtle, mida see tähendab.

```bash
# Ainult fct_posts testid
docker compose exec dbt dbt test --select fct_posts
```

### Aruteluks

- Millal peaks test andma hoiatuse (WARN) ja millal vea (FAIL)?
- Kuidas käsitleksid testide ebaõnnestumist tootmiskeskkonnas?
- Miks on mõttekas testida marts kihis, mitte ainult staging kihis?

### Näidislahendus

`ylesanne2_assert_no_orphan_posts.sql`  
ja
`ylesanne2_schema.yml___` (asenda samas kaustas oleva `schema.yml`-ga)

---

## Ülesanne 3: Kasutaja aktiivsusraport (~15 min)

### Taust

Äripool küsib raportit, mis näitab iga kasutaja kohta: kes ta on, kus ta praegu elab, kus ta enne elas (kui aadress on muutunud), ja kui aktiivne ta on. See on tüüpiline reaalne päring, kus kombineeritakse dimensioonid, meetrikad ja ajalugu.

### Ülesanne

Loo fail `models/marts/user_activity_report.sql` (materialiseerimine: `table`).

### Nõuded

- **Veerud:**
  - `user_key`, `full_name`, `email`
  - `current_city`, `current_country` — praegune aadress (snapshotist, `dbt_valid_to = '9999-12-31'`)
  - `previous_city`, `previous_country` — eelmine aadress (snapshotist, viimane suletud versioon). `NULL`, kui aadressi pole muudetud.
  - `total_posts` — postituste arv (0, kui postitusi pole)
  - `avg_post_length` — keskmine postituse pikkus
  - `registered_date`

### Vihjed

- Kasuta CTE-sid (Common Table Expressions): üks praeguse aadressi jaoks, teine eelmise jaoks
- Praegune aadress: `WHERE dbt_valid_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')`
- Eelmine aadress: `WHERE dbt_valid_to != to_timestamp('9999-12-31', 'YYYY-MM-DD')`, sorteeri `dbt_valid_from DESC` ja võta esimene (nt `ROW_NUMBER() OVER (PARTITION BY uuid ORDER BY dbt_valid_from DESC)`)
- Snapshotitabel on `{{ ref('snap_users') }}`
- Postituste meetrikad: arvuta `fct_posts` tabelist `GROUP BY user_key`
- Kasuta `COALESCE(pm.total_posts, 0)` puuduvate väärtuste jaoks
- `LEFT JOIN` kõik CTE-d `dim_users` tabeliga

### Sammud

1. Loo fail `models/marts/user_activity_report.sql`
2. Käivita:
   ```bash
   docker compose exec dbt dbt run --select user_activity_report
   ```
3. Kontrolli tulemust:
   ```bash
   docker compose exec db psql -U praktikum -c "SELECT full_name, current_city, previous_city, total_posts FROM marts.user_activity_report ORDER BY total_posts DESC;"
   ```

Kasutajad, kelle aadressi demo ajal muutsime, peaksid näitama nii `current_city` kui `previous_city`. Teistel on `previous_city = NULL`.

### Aruteluks

- Miks on see muster (dimensioon + meetrikad + ajalugu) ärianalüüsis nii levinud?
- Mis juhtub, kui snapshot-tabel kustutatakse ja luuakse uuesti? Mida kaotame?
- Kas see raport peaks olema `view` või `table`? Mis on kompromissid?

### Näidislahendus

`ylesanne3_user_activity_report.sql` 

---

## Levinud vead ja tõrkeotsing

### dbt debug ebaõnnestub

**Sümptom:** `dbt debug` näitab ühenduse viga.

**Diagnostika:**
```bash
# Kas andmebaas töötab?
docker compose ps

# Kas ühendus toimib?
docker compose exec db psql -U praktikum -c "SELECT 1;"
```

**Lahendus:** Kontrolli, et `profiles.yml` failis on õiged keskkonnamuutujad ja et `db` teenus on üleval.

### Source not found

**Sümptom:** `dbt run` annab vea `source staging.users not found`.

**Diagnostika:**
```bash
# Kas staging skeema ja tabelid eksisteerivad?
docker compose exec db psql -U praktikum -c "\dt staging.*"
```

**Lahendus:** Käivita esmalt Python-skript andmete laadimiseks:
```bash
docker compose exec python python ingest.py users
docker compose exec python python ingest.py posts
```

### Snapshot-tabel puudub

**Sümptom:** Ülesanne 3 annab vea `relation "snapshots.snap_users" does not exist`.

**Lahendus:** Snapshot tuleb enne käivitada:
```bash
docker compose exec dbt dbt snapshot
```

### Inkrementaalne mudel lisab kõik read uuesti

**Sümptom:** Pärast teist käivitust on ridu rohkem kui oodatud.

**Diagnostika:** Vaata, kas `unique_key` on õige ja kas `is_incremental()` blokk filtreerib õigesti.

**Lahendus:** Kontrolli, et `unique_key` vastab tegelikule unikaalsele veerule (või veergude kombinatsioonile). Proovi `--full-refresh` ja alusta uuesti.

### Pordikonflikt

**Sümptom:** `docker compose up` annab vea `port 5432 already in use`.

**Lahendus:** Peata eelmiste praktikumide konteinerid:
```bash
# Teises praktikumikaustas:
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
