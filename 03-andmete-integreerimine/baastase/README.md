# Praktikum 3: Andmete integreerimine API ja CSV abil

## Sisukord

- [Praktikumi eesmärk](#praktikumi-eesmärk)
- [Õpiväljundid](#õpiväljundid)
- [Hinnanguline ajakulu](#hinnanguline-ajakulu)
- [Eeldused](#eeldused)
- [Enne alustamist](#enne-alustamist)
- [Praktikumi failid](#praktikumi-failid)
- [Kus praktikumi failid asuvad?](#kus-praktikumi-failid-asuvad)
- [Miks see teema on oluline?](#miks-see-teema-on-oluline)
- [Uued mõisted](#uued-mõisted)
- [ETL etapid selles praktikumis](#etl-etapid-selles-praktikumis)
- [Soovitatud töötee](#soovitatud-töötee)
- [1. Ava õige kaust](#1-ava-õige-kaust)
- [2. Loo `.env` fail](#2-loo-env-fail)
- [3. Käivita konteinerid](#3-käivita-konteinerid)
- [4. Vaata üle põhiallikad](#4-vaata-üle-põhiallikad)
- [5. Loo skeemid, tabelid ja vaated](#5-loo-skeemid-tabelid-ja-vaated)
- [6. Laadi CSV-fail staging-tabelisse](#6-laadi-csv-fail-staging-tabelisse)
- [7. Vaata valmis ETL-skripti ja vaateid](#7-vaata-valmis-etl-skripti-ja-vaateid)
- [8. Käivita ETL](#8-kaivita-etl)
- [9. Kontrolli tulemust SQL-iga](#9-kontrolli-tulemust-sql-iga)
- [10. Kontrolli idempotentsust](#10-kontrolli-idempotentsust)
- [Kontrollpunktid](#kontrollpunktid)
- [Levinud vead ja lahendused](#levinud-vead-ja-lahendused)
- [Kokkuvõte](#kokkuvõte)
- [Lisaülesanded](#lisaülesanded)
- [Koristamine](#koristamine)

## Praktikumi eesmärk

Selle praktikumi eesmärk on teha läbi esimene töötav andmete integreerimise töövoog nii, et sa ei peaks Pythoni osa ise nullist kirjutama.

Põhivoos kasutame kahte allikat:

- avalikku `API`-t;
- kohalikku `CSV` faili.

Kolmas allikas, kohalik `JSON` fail, jääb lisaülesandesse. Seal saad olemasoleva töötava näite põhjal proovida, kuidas laiendada sama töövoogu kolme allika peale.

## Õpiväljundid

Praktikumi lõpuks oskad:

- käivitada baastaseme 3. praktikumi keskkonna `docker compose` abil;
- laadida `CSV` faili staging-tabelisse;
- käivitada valmis Pythoni `ETL` skripti, mis loeb andmeid `API`-st;
- selgitada, kuidas `API` ja `CSV` andmed ühendatakse ühe võtme alusel;
- kontrollida `SQL` päringutega, kas andmed jõudsid õigesse tabelisse;
- kirjeldada, miks sama töö saab turvaliselt uuesti käivitada.

## Hinnanguline ajakulu

Arvesta umbes 2 tunniga.

See aeg jaguneb ligikaudu nii:

- 20 min keskkonna käivitamiseks ja failidega tutvumiseks;
- 25 min skeemide ja tabelite loomiseks;
- 20 min `CSV` faili laadimiseks ja kontrollimiseks;
- 25 min valmis `ETL` skripti läbivaatamiseks ja käivitamiseks;
- 30 min tulemuse kontrollimiseks ja korduskäivituse proovimiseks.

Lisaülesanded ei pea mahtuma praktikumi põhiaja sisse.

Kui teed lisaülesande 1, arvesta juurde umbes 60 kuni 90 minutit.

Kui teed võtmete kontrolli lisaülesande läbi, arvesta juurde umbes 30 kuni 45 minutit.

Kui teed Parquet lisaülesande läbi, arvesta juurde umbes 30 kuni 45 minutit.

Kui teed kõik lisaülesanded läbi, arvesta põhirajale lisaks umbes 3,5 kuni 5 tundi.

## Eeldused

Sul on vaja:

- VS Code'i või GitHub Codespacesit;
- terminali;
- töötavat Dockeri keskkonda, kui teed praktikumi oma arvutis;
- selle repositooriumi faile.

See praktikum eeldab, et eelmistest baastaseme praktikumidest on tuttavad vähemalt järgmised sammud:

- oskad avada õige kausta;
- oskad luua `.env` faili `.env.example` põhjal;
- oskad käivitada käsu `docker compose up -d`;
- oskad avada `psql` kliendi käsuga `docker compose exec db psql ...`;
- tead, et osa käske käib hosti terminalis ja osa `psql` sees.

Kui need sammud on veel ebakindlad, vaata vajadusel uuesti:

- [Praktikum 1: PostgreSQL-iga ühenduse loomine ja esimese CSV-faili laadimine](../../01-andmeinseneeria-alused/baastase/README.md)
- [Praktikum 2: Lihtne faktitabel ja kaks dimensiooni](../../02-andmemudelid-ja-baasid/baastase/README.md)

## Enne alustamist

### Soovitatud keskkond

Praktikumi tegemiseks sobib hästi järgmine tööviis:

- avad kausta `03-andmete-integreerimine/baastase` VS Code'is;
- kasutad VS Code'i sisseehitatud terminali;
- käivitad kõik käsud samas terminaliaknas;
- avad `SQL`, `CSV`, `JSON` ja `Python` failid VS Code'is.

See tööviis aitab hoida failid, käsud ja väljundid ühes kohas.

Selle praktikumi saab läbida ka GitHub Codespacesis.

Kui töötad Codespacesis, siis selle praktikumi kaust on tavaliselt:

```text
/workspaces/ut-andmeinseneeria-2026/03-andmete-integreerimine/baastase
```

Kasulikud lingid:

- repositoorium: <https://github.com/KristoR/ut-andmeinseneeria-2026>
- sinu Codespacesi sessioonid: <https://github.com/codespaces>

Selles kursuses kasutab iga praktikum andmebaasi jaoks eri porti. Selle praktikumi port on `5434`.

Sellel praktikumil on ka oma Dockeri andmemaht. Nii ei lähe eelmiste nädalate andmebaasi sisu selle nädala omaga segamini.

Kui tahad selle praktikumi andmebaasi täiesti puhtalt uuesti alustada, kasuta juhendi lõpus käsku `docker compose down -v`.

### Kuidas saada kätte selle nädala failid?

Kui sul on repositoorium juba eelmiste nädalate tööde jaoks olemas, uuenda enne alustamist failid.

Kui töötad oma isikliku fork'iga, tee enne kohalikus koopias uuendamist GitHubis `Sync fork`.

Kui kasutasid `git clone` käsku:

1. kontrolli olekut:

```bash
git status
```

2. kontrolli, et kohalik repositoorium oleks harus `main`:

```bash
git branch --show-current
```

Kui töötad parajasti mõnes teises harus ja seal on sinu kohalikud muudatused, siis tee need enne haru vahetamist commit'iks.

Kui `git branch --show-current` näitab mõnda muud haru, vaheta kõigepealt `main` harule:

```bash
git checkout main
```

3. uuenda failid:

```bash
git pull
```

Kui `git pull` ütleb, et sul on pooleli kohalikke muudatusi ja sa ei tea, kuidas neid lahendada, siis kõige turvalisem tee on teha praktikumi jaoks uus puhas koopia.

Kui laadisid repositooriumi varem alla `ZIP` failina:

1. ava repositoorium GitHubis;
2. vali `Code`;
3. vali `Download ZIP`;
4. paki fail lahti uude kausta;
5. ava see uus kaust VS Code'is.

## Praktikumi failid

Kõik allpool toodud suhtelised failiteed eeldavad, et asud kaustas `03-andmete-integreerimine/baastase`.

- [`compose.yml`](./compose.yml) kirjeldab andmebaasi- ja Pythoni konteinerit
- [`.env.example`](./.env.example) sisaldab ühenduse näidisväärtusi
- [`Dockerfile.python`](./Dockerfile.python) ehitab Pythoni konteineri
- [`data/kasutaja_staatus.csv`](./data/kasutaja_staatus.csv) on põhiraja `CSV` allikas
- [`data/teavituseelistused.json`](./data/teavituseelistused.json) on lisaülesande `JSON` allikas
- [`data/kasutaja_rikastus.parquet`](./data/kasutaja_rikastus.parquet) on lisaülesande `Parquet` snapshot
- [`scripts/01_create_tables.sql`](./scripts/01_create_tables.sql) loob põhiraja skeemid, tabelid ja `intermediate` vaated
- [`scripts/02_load_user_status.sql`](./scripts/02_load_user_status.sql) laadib `CSV` faili `staging` tabelisse
- [`scripts/03_check_staging.sql`](./scripts/03_check_staging.sql) aitab staging-andmeid kontrollida
- [`scripts/03_integrate_users.py`](./scripts/03_integrate_users.py) loeb `API` kasutajad sisse, laadib need `staging` kihti ja täidab lõpptabeli `intermediate` vaate kaudu
- [`scripts/04_check_results.sql`](./scripts/04_check_results.sql) kontrollib `staging`, `intermediate` ja `analytics` kihi tulemusi
- [`scripts/lisa_01_prepare_preferences.sql`](./scripts/lisa_01_prepare_preferences.sql) valmistab lisaülesande jaoks ette kolmanda allika tabeli, vaate ja lõppväljad
- [`scripts/lisa_03_integrate_users_template.py`](./scripts/lisa_03_integrate_users_template.py) on lisaülesande mall kolme allika voole `staging -> intermediate -> analytics`
- [`scripts/lisa_04_check_join_keys.py`](./scripts/lisa_04_check_join_keys.py) kontrollib, kas `staging` tabelite võtmed sobituvad
- [`scripts/lisa_05_preview_parquet.sql`](./scripts/lisa_05_preview_parquet.sql) näitab, kuidas `Parquet` failist otse pärida
- [`scripts/lisa_07_load_loyalty_snapshot.sql`](./scripts/lisa_07_load_loyalty_snapshot.sql) loob `Parquet` snapshot'i põhjal staging-vaate
- [`scripts/lisa_08_check_loyalty_snapshot.sql`](./scripts/lisa_08_check_loyalty_snapshot.sql) kontrollib staging-vaate ja lõpptabeli seost
- [`scripts/naidis_lahendused/lisa_03_integrate_users_lahendus.py`](./scripts/naidis_lahendused/lisa_03_integrate_users_lahendus.py) on üks võimalik lahendus lisaülesandele
- [`scripts/99_reset.sql`](./scripts/99_reset.sql) eemaldab praktikumi skeemid
- [`scripts/requirements.txt`](./scripts/requirements.txt) loetleb Pythoni teegid

## Kus praktikumi failid asuvad?

Selles praktikumis on korraga kasutusel kolm konteksti.

- Host ehk sinu arvuti või Codespace'i tööruum
- Andmebaasi konteiner `db`
- Pythoni konteiner `python`

Sama fail võib olla eri kontekstides eri teega.

Näited:

- hostis on fail `data/kasutaja_staatus.csv`
- andmebaasi konteineri sees on sama fail `/data/kasutaja_staatus.csv`
- Pythoni konteineri sees on sama fail samuti `/data/kasutaja_staatus.csv`

Sama kehtib `scripts` kausta kohta:

- hostis on fail `scripts/03_integrate_users.py`
- konteineri sees on see fail `/scripts/03_integrate_users.py`

Kui kasutad Windowsis `Git Bash`-i ja annad konteineri sisetee hosti terminalis käsule `docker compose exec` argumendina kaasa, siis võib `Git Bash` proovida algavat kaldkriipsu oma moodi tõlgendada.

Selle vältimiseks kirjuta selline tee kujul `//scripts/...` või `//data/...`.

Näiteks:

```bash
docker compose exec python python //scripts/03_integrate_users.py
```

See märkus puudutab ainult hosti terminalis käivitatavaid `docker compose exec` käske. Kui oled juba `psql` sees, kasuta endiselt tavalist konteineriteed kujul `/scripts/...`.

See vahe on oluline kahel põhjusel:

- `psql` loeb `\copy` käsus faili konteineri vaatest;
- Pythoni skript ühendub andmebaasiga konteineri sisevõrgu kaudu nimega `db`.

## Miks see teema on oluline?

Tööelus tuleb väga sageli ette olukord, kus üks allikas on süsteemi `API` ja teine allikas on fail.

Näiteks:

- äri- või registrisüsteem annab sulle `API` vastuse;
- mõni teine süsteem saadab kõrvale väikese `CSV` ekspordi.

Andmete integreerimine tähendab siin vähemalt nelja asja:

- lugeda andmed erinevatest allikatest sisse;
- leida ühine võti, mille järgi neid seostada;
- puhastada see võti enne ühendamist;
- laadida tulemus tabelisse, mida saab hiljem pärida.

Selles praktikumis teeme just selle väikese põhiloogika läbi.

## Uued mõisted

### `API`

`API` tähendab `Application Programming Interface`, eesti keeles rakendusliidest.

Selles praktikumis tähendab see kohta, kust Pythoni skript saab kasutajate andmed `HTTP` päringuga kätte.

### `CSV`

`CSV` tähendab `Comma-Separated Values`, eesti keeles komadega eraldatud tabelandmeid tekstifailis.

Selles praktikumis tuleb `CSV` failist kasutaja staatus.

### `Staging`

`Staging` on vahekiht, kuhu paneme andmed enne lõplikku kasutustabelit.

Selles praktikumis kasutame `staging` skeemi selleks, et hoida:

- `CSV` failist tulnud kasutajastaatused;
- `API`-st tulnud kasutajad.

Selles praktikumis on `staging` algallikale lähedane maandumiskiht. Siin hoiame andmeid veel üsna allikalähedasel kujul, enne kui neid puhastame, ühendame ja laadime lõpptabelisse.

Edasijõudnute rajal jääb `staging` samuti allikalähedaseks kihiks, kuid selle ja lõpptabelite vahele tuleb veel eraldi `intermediate` kiht puhastamise ja ümberkujundamise jaoks.

Selles baastaseme praktikumis kasutame lõppkihi kohta nime `analytics`. Mõnes teises projektis või edasijõudnute rajal võib sama lõppkihti kohata nimega `marts`. Need tähistavad sisuliselt sama kihti; ühe projekti sees tasub siiski hoida terminid ühtlasena.

### `Intermediate`

`Intermediate` on vahekiht, kus andmed ei ole enam päris allikalähedased, aga ei ole veel ka lõppkasutuseks valmis.

Selles praktikumis teeb `intermediate` kiht kolm asja:

- puhastab e-posti võtme võrreldavale kujule;
- seob põhiallika rikastavate allikatega;
- valmistab andmed ette lõpptabelisse laadimiseks.

### Idempotentsus

Idempotentsus tähendab, et sama töö korduv käivitamine annab sama lõpptulemuse.

Selles praktikumis saavutame selle lihtsal viisil:

- tühjendame väikesed sihttabelid enne uut laadimist;
- laeme samad andmed uuesti sisse.

See ei ole ainus võimalik lahendus, aga baastasemel on see hästi jälgitav.

## ETL etapid selles praktikumis

Selles praktikumis vaatame `ETL`-i kogu töövoona, mis viib andmed ühest kihist teise.

Põhirajal toimub töö järgmises järjekorras:

1. `CSV` fail laetakse tabelisse `staging.user_status`
2. Pythoni skript loeb kasutajad `API`-st ja laadib need tabelisse `staging.api_users`
3. `intermediate` vaated puhastavad võtme ja seovad kaks allikat
4. lõpptulemus laaditakse tabelisse `analytics.user_profile`

### `Andmete vastuvõtt`

Andmete kättesaamine allikatest.

Selles praktikumis tähendab see:

- kasutajate lugemist `API`-st;
- kasutajastaatuste vastuvõttu `CSV` failist.

### `Töötlus`

Andmete puhastamine ja ühendamine.

Selles praktikumis tähendab see:

- e-posti võtme puhastamist `intermediate` vaadetes;
- vajalike väljade valimist;
- `API` ja `CSV` andmete ühendamist ühe e-posti võtme alusel.

### `Laadimine`

Andmete kirjutamist sihtkohta.

Selles praktikumis tähendab see:

- allikate maandamist `staging` kihti;
- lõpptulemuse salvestamist tabelisse `analytics.user_profile`.

## Soovitatud töötee

Praktikumi tööjärjekord on järgmine.

1. Ava õige kaust.
2. Loo `.env` fail.
3. Käivita konteinerid.
4. Vaata üle põhiraja allikad.
5. Loo skeemid, tabelid ja vaated.
6. Laadi `CSV` tabelisse.
7. Vaata valmis `ETL` skript ja vaated läbi.
8. Käivita töö.
9. Kontrolli tulemust.

See tööjärjekord hoiab põhiraja lihtsa ja jälgitavana. Pythoni koodi täiendamine tuleb alles lisaülesandes.

## 1. Ava õige kaust

See samm tehakse hosti terminalis.

Liigu praktikumi kausta:

```bash
cd 03-andmete-integreerimine/baastase
```

Soovi korral kontrolli asukohta.

macOS-is, Linuxis ja Codespacesis:

```bash
pwd
```

Windows PowerShellis:

```powershell
Get-Location
```

## 2. Loo `.env` fail

See samm tehakse hosti terminalis.

macOS-is, Linuxis ja Codespacesis:

```bash
cp .env.example .env
```

Windows PowerShellis:

```powershell
Copy-Item .env.example .env
```

Vaikimisi väärtused on:

- kasutaja `praktikum`
- parool `praktikum`
- andmebaas `praktikum`

Selles praktikumis ei ole vaja neid muuta.

## 3. Käivita konteinerid

See samm tehakse hosti terminalis.

Käivita andmebaas ja Pythoni konteiner:

```bash
docker compose up -d --build
```

Kontrolli, et teenused käivitusid:

```bash
docker compose ps
```

Oodatav tulemus:

- teenus `db` on olekus `running` või `healthy`
- teenus `python` on olekus `running`

Kui tahad näha andmebaasi viimaseid logisid:

```bash
docker compose logs db --tail=20
```

## 4. Vaata üle põhiallikad

See samm tehakse kahes kohas:

- hostis VS Code'i failivaates
- veebibrauseris

Põhiraja kaks allikat on:

1. avalik `API`: ava brauseris `https://jsonplaceholder.typicode.com/users`
2. kohalik `CSV` fail [`data/kasutaja_staatus.csv`](./data/kasutaja_staatus.csv): ava see VS Code'is

Vaata need lühidalt üle ja pööra tähelepanu järgmisele:

- `CSV` failis on e-posti aadressides eri kujusid, näiteks suured tähed ja tühikud;
- `API` vastuses on kasutaja linn ja ettevõtte nimi pesastatud kujul;
- mõlemat allikat saab ühendada e-posti alusel, aga enne tuleb e-post puhastada.

Sul ei ole vaja kõiki välju detailselt läbi analüüsida. Piisab sellest, kui märkad, millised väljad on olemas ja milline väli sobib ühendusvõtmeks.

Samas kaustas on ka fail [`data/teavituseelistused.json`](./data/teavituseelistused.json), kuid põhirajal me seda veel ei kasuta. See tuleb mängu lisaülesandes.

## 5. Loo skeemid, tabelid ja vaated

See samm tehakse kõigepealt hosti terminalis, seejärel `psql` sees.

Ava `psql` andmebaasi konteineri sees:

```bash
docker compose exec db psql -U praktikum -d praktikum
```

Kui `psql` on avanenud, käivita tabelite loomise skript:

```sql
\i /scripts/01_create_tables.sql
```

Kontrolli, et skeemid tekkisid:

```sql
\dn
```

Kontrolli, et tabelid tekkisid:

```sql
\dt staging.*
\dt analytics.*
```

Kontrolli, et `intermediate` vaated tekkisid:

```sql
\dv intermediate.*
```

Oodatav tulemus:

- skeemid `staging`, `intermediate` ja `analytics`
- tabelid `staging.user_status`, `staging.api_users` ja `analytics.user_profile`
- vaated `intermediate.api_users_normalized`, `intermediate.user_status_normalized` ja `intermediate.user_profile_enriched`

## 6. Laadi CSV-fail staging-tabelisse

See samm tehakse endiselt `psql` sees.

See on töövoo ettevalmistav samm, mis teeb `CSV` allika skriptile kättesaadavaks.

Põhiraja `ETL` loogikas on see osa sellest, kuidas teine allikas jõuab staging-kihti enne ühendamist.

Käivita `CSV` laadimise skript:

```sql
\i /scripts/02_load_user_status.sql
```

Siis kontrolli, mida tabelisse laeti:

```sql
\i /scripts/03_check_staging.sql
```

Pööra tähelepanu sellele, et e-posti väljad ei ole veel puhastatud.

Staging-tabelisse võivadki jõuda toorandmed täpselt sellisena, nagu need allikast tulid.

Kui oled kontrolli lõpetanud, välju `psql`-ist:

```sql
\q
```

## 7. Vaata valmis ETL-skripti ja vaateid

See samm tehakse hostis VS Code'i redaktoris.

Ava failid [`scripts/03_integrate_users.py`](./scripts/03_integrate_users.py) ja [`scripts/01_create_tables.sql`](./scripts/01_create_tables.sql).

Sa ei pea selles etapis midagi muutma. Eesmärk on saada aru, kuidas töövoog jaguneb kihtidesse.

Pythoni fail näitab, kuidas `API` andmed vastu võetakse ja `staging` kihti laaditakse. `SQL` fail näitab, kuidas `intermediate` vaated võtmed puhastavad ja allikad seovad.

Vaata läbi neli kohta. Need vastavad töövoo etappidele nii:

1. `Andmete vastuvõtt`: `fetch_api_users` loeb kasutajad `API`-st
2. `Laadimine staging kihti`: `load_api_users` laadib `API` kasutajad tabelisse `staging.api_users`
3. `Töötlus`: vaated `intermediate.api_users_normalized`, `intermediate.user_status_normalized` ja `intermediate.user_profile_enriched` puhastavad võtme ja ühendavad andmed
4. `Laadimine analytics kihti`: `load_final_rows_from_intermediate` laadib lõpptabeli `intermediate` vaatest

Nii näed, et allikad maanduvad kõigepealt `staging` kihti, seos tehakse `intermediate` kihis ja lõpptabel täidetakse alles viimasena.

## 8. Käivita ETL

See samm tehakse hosti terminalis.

Käivita valmis skript:

```bash
docker compose exec python python //scripts/03_integrate_users.py
```

Oodatav tulemus on umbes selline:

```text
ETL etapp 1/3: Andmete vastuvõtt ja laadimine staging kihti
- API-st tuli 10 kasutajat.
- Laadisin staging.api_users tabelisse 10 rida.
- staging.user_status tabelis on 10 staatusekirjet.
ETL etapp 2/3: Töötlus
- Intermediate vaade puhastas e-posti ja ühendas andmed 10 kasutaja jaoks.
ETL etapp 3/3: Laadimine analytics kihti
- Laadisin analytics.user_profile tabelisse 10 rida.
Valmis.
```

Kui skript annab veateate, loe see rahulikult läbi.

Kõige sagedasemad vead selles kohas on:

- `API` päring ei õnnestu;
- andmebaasi tabelid ei ole veel loodud;
- `CSV` faili pole enne staging-tabelisse laetud.

## 9. Kontrolli tulemust SQL-iga

See samm tehakse taas `psql` sees.

Ava `psql`:

```bash
docker compose exec db psql -U praktikum -d praktikum
```

Käivita kontrollpäringud:

```sql
\i /scripts/04_check_results.sql
```

Vaata eriti järgmisi küsimusi:

- kas `staging.api_users` sisaldab 10 rida;
- kas `intermediate.user_profile_enriched` sisaldab 10 rida;
- kas `analytics.user_profile` sisaldab 10 rida;
- kas `intermediate` ja `analytics` kihis on e-posti aadressid väikeste tähtedega ja ilma üleliigsete tühikuteta;
- kas mõnel kasutajal on `account_status` puudu.

See viimane küsimus on oluline. Integreerimisel ei olegi alati igas allikas kõigi kirjete kohta täielikku infot.

Selles andmestikus on selline puudulik sobitumine ootuspärane.

Oodatav tulemus on järgmine:

- `intermediate.user_profile_enriched` vaates on 10 kasutajat;
- lõpptabelis on 10 kasutajat;
- neist 7 kasutajal on `account_status` olemas;
- 3 kasutajal jääb `account_status` väärtus `NULL`.

See juhtub seetõttu, et `CSV` failis on kolm e-posti võtit, mis ei sobitu `API` võtmetega:

- kirjaveaga `maxiime_nienow@alicia.info`;
- eraldi võtmega `delphine@adams.us`;
- eraldi võtmega `puuduv.kasutaja@example.com`.

Seetõttu jääb `account_status` puudu kasutajatel `sherwood@rosamond.me`, `chaim_mcdermott@dana.io` ja `rey.padberg@karina.biz`.

See aitab näha, et andmete integreerimine ei tähenda ainult ühendamist, vaid ka allikate kvaliteedi kontrolli.

Kui oled tulemuse üle vaadanud, välju `psql`-ist:

```sql
\q
```

## 10. Kontrolli idempotentsust

See samm tehakse hosti terminalis.

Käivita sama skript teist korda:

```bash
docker compose exec python python //scripts/03_integrate_users.py
```

Seejärel kontrolli, et ridade arv ei kasvanud:

```bash
docker compose exec db psql -U praktikum -d praktikum -c "SELECT COUNT(*) FROM analytics.user_profile;"
```

Oodatav tulemus on ikka `10`.

Mida see näitab?

- töö on selles väikeses näites idempotentne;
- korduv käivitamine ei tekita duplikaate;
- andmetoru saab vajadusel uuesti käivitada.

## Kontrollpunktid

Praktikumi keskel ja lõpus saad end kontrollida nende punktide abil.

### Pärast konteinerite käivitamist

- `docker compose ps` näitab teenuseid `db` ja `python`
- `db` teenus on `healthy` või `running`

### Pärast tabelite loomist

- `\dn` näitab skeeme `staging`, `intermediate` ja `analytics`
- `\dt staging.*` näitab kahte staging-tabelit
- `\dv intermediate.*` näitab kolme `intermediate` vaadet

### Pärast `CSV` laadimist

- `staging.user_status` sisaldab 10 rida
- tabelis on näha puhastamata e-posti väljad

### Pärast `ETL` skripti käivitamist

- `staging.api_users` sisaldab 10 rida
- `intermediate.user_profile_enriched` sisaldab 10 rida
- `analytics.user_profile` sisaldab 10 rida

## Levinud vead ja lahendused

See jaotis katab selle praktikumi kõige tõenäolisemad komistuskohad. Kui jääd hätta käsurea, `psql` või Dockeriga tehtavate põhisammudega, vaata vajadusel uuesti ka [praktikum 1 juhendit](../../01-andmeinseneeria-alused/baastase/README.md) ja [praktikum 2 juhendit](../../02-andmemudelid-ja-baasid/baastase/README.md).

### Sümptom: `no configuration file provided: not found`

Tõenäoline põhjus:

- käivitasid `docker compose` käsu vales kaustas

Lahendus:

- kontrolli käsuga `pwd` või `Get-Location`, kus sa parasjagu asud
- liigu kausta `03-andmete-integreerimine/baastase`
- käivita siis käsk `docker compose up -d --build`

### Sümptom: `.env` faili ei leita

Tõenäoline põhjus:

- fail `.env` on veel loomata

Lahendus:

- loo fail käsuga `cp .env.example .env`
- Windows PowerShellis kasuta käsku `Copy-Item .env.example .env`
- käivita siis `docker compose up -d --build` uuesti

### Sümptom: `docker compose up -d --build` ebaõnnestub

Tõenäoline põhjus:

- Docker ei tööta või ei ole käivitatud

Lahendus:

- ava Docker Desktop või kontrolli, et sinu Dockeri teenus töötab
- proovi käsku uuesti

### Sümptom: `Bind for 0.0.0.0:5434 failed` või `port is already allocated`

Tõenäoline põhjus:

- hostis on port `5434` juba mõne teise teenuse kasutuses

Lahendus:

- peata teine teenus, mis sama porti kasutab
- või muuda failis [`compose.yml`](./compose.yml) real `5434:5432` vasakpoolne number mõneks vabaks pordiks, näiteks `5544:5432`
- pärast muudatust käivita `docker compose up -d --build` uuesti

### Sümptom: `could not translate host name "db"`

Tõenäoline põhjus:

- käivitasid Pythoni väljaspool konteinerit

Lahendus:

- kasuta käsku `docker compose exec python python //scripts/03_integrate_users.py`
- ära käivita seda skripti oma hosti Pythoniga

### Sümptom: `python: can't open file` või `Git Bash` tõlgendab tee valesti

Tõenäoline põhjus:

- kasutad Windowsis `Git Bash`-i
- käivitasid `docker compose exec` käsu nii, et konteineri sisetee algas kujul `/scripts/...`

Lahendus:

- kasuta hosti terminalis konteineri siseteed kujul `//scripts/...` või `//data/...`
- näiteks käsk `docker compose exec python python //scripts/03_integrate_users.py`
- kui oled juba `psql` sees, kasuta seal edasi tavalist teed kujul `/scripts/...`

### Sümptom: `relation "staging.user_status" does not exist`

Tõenäoline põhjus:

- tabelid ei ole veel loodud

Lahendus:

- ava `psql`
- käivita `\i /scripts/01_create_tables.sql`
- alles siis lae `CSV` ja käivita `ETL` skript

### Sümptom: `relation "intermediate.user_profile_enriched" does not exist`

Tõenäoline põhjus:

- põhiraja skeemid ja vaated ei ole veel loodud

Lahendus:

- ava `psql`
- käivita `\i /scripts/01_create_tables.sql`
- kontrolli käsuga `\dv intermediate.*`, et vaated tekkisid
- käivita siis `ETL` skript uuesti

### Sümptom: `analytics.user_profile` jääb tühjaks või väljad on `NULL`

Tõenäoline põhjus:

- `CSV` faili pole staging-tabelisse laetud või `staging.api_users` on tühi

Lahendus:

- kontrolli, et käivitasid faili `\i /scripts/02_load_user_status.sql`
- käivita `ETL` skript uuesti, et `API` andmed jõuaksid tabelisse `staging.api_users`
- vaata päringuga `SELECT * FROM intermediate.user_profile_enriched ORDER BY user_id;`, kuidas võtmed `intermediate` kihis kokku sobituvad

### Sümptom: lisaülesandes loodud tabel või vaade tekib skeemis `public` või läheb sama nimega objektiga konflikti

Tõenäoline põhjus:

- lõid uue tabeli või vaate ilma skeemi nimeta
- kasutasid liiga üldist nime, näiteks `users`, `results` või `snapshot`

Lahendus:

- kasuta praktikumi olemasolevaid skeeme `staging` ja `analytics`
- kasuta vajadusel ka skeemi `intermediate`
- kirjuta objekti nimi alati koos skeemiga, näiteks `staging.user_loyalty_snapshot`
- kontrolli tabeleid käskudega `\dt staging.*` ja `\dt analytics.*`
- kontrolli vaateid käskudega `\dv staging.*` ja `\dv intermediate.*`

## Kokkuvõte

Selles praktikumis tegid läbi esimese töötava andmete integreerimise toru.

Töövoog oli järgmine:

1. lõid skeemid, tabelid ja `intermediate` vaated;
2. laadisid `CSV` faili tabelisse `staging.user_status`;
3. lugesid kasutajad `API`-st ja laadisid need tabelisse `staging.api_users`;
4. puhastasid võtme ja ühendasid allikad `intermediate` kihis;
5. laadisid lõpptulemuse tabelisse `analytics.user_profile`.

See on lihtne, aga päris `ETL` näide. Suuremates süsteemides on samme rohkem, kuid põhiloogika on sama.

## Lisaülesanded

Need ülesanded ei pea mahtuma praktikumi põhiaja sisse.

Lisaülesannetes on juhiseid vähem kui põhirajal. Nendes ülesannetes saad olemasolevat lahendust väikeste sammudega ise laiendada.

### Lisaülesanne 1: lisa kolmas allikas ja täienda skripti

Selles ülesandes kasutad ka faili [`data/teavituseelistused.json`](./data/teavituseelistused.json).

Ka kolme allika variandis jääb töövoo loogika samaks:

- allikad maanduvad esmalt `staging` kihti;
- puhastus ja ühendamine toimub `intermediate` vaates;
- lõpptulemus laetakse tabelisse `analytics.user_profile`.

Tee nii:

1. valmista andmebaas ette `psql` sees:

```sql
\i /scripts/lisa_01_prepare_preferences.sql
```

See skript teeb kolm asja:

- loob tabeli `staging.notification_preferences`;
- lisab lõpptabelisse uued väljad;
- laiendab `intermediate.user_profile_enriched` vaadet nii, et see ühendaks ka `JSON` allika.

2. tee mallist oma tööfail:

macOS-is, Linuxis ja Codespacesis:

```bash
cp scripts/lisa_03_integrate_users_template.py scripts/lisa_03_integrate_users.py
```

Windows PowerShellis:

```powershell
Copy-Item scripts/lisa_03_integrate_users_template.py scripts/lisa_03_integrate_users.py
```

3. täienda failis `scripts/lisa_03_integrate_users.py` märgitud funktsioonid
4. käivita uus skript hosti terminalis:

```bash
docker compose exec python python //scripts/lisa_03_integrate_users.py
```

Selles lisaülesandes teed ise sama töövoo järgmise sammu läbi: liigud kahelt allikalt kolme allikani, aga hoiad kihid endiselt eraldi.

Pärast oma katset võid võrrelda tulemust failiga:

- [`scripts/naidis_lahendused/lisa_03_integrate_users_lahendus.py`](./scripts/naidis_lahendused/lisa_03_integrate_users_lahendus.py)

### Lisaülesanne 2: lisa telefoninumber lõpptabelisse

Põhiskript ei vii `API` vastusest telefoninumbrit veel lõpptabelisse.

Tee nii:

- lisa väli `phone` tabelisse `staging.api_users`;
- lisa väli `phone` tabelisse `analytics.user_profile`;
- lisa telefoninumber `API` andmete hulgast `staging` kihti;
- lisa telefoninumber `intermediate` vaatesse;
- lae see ka lõpptabelisse.

Vihje: selles ülesandes on mõistlik täiendada olemasolevat tabelit käsuga `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, mitte luua kogu tabelit uuesti.

Vihje: kui teed seda põhirajal, muuda faile [`scripts/01_create_tables.sql`](./scripts/01_create_tables.sql) ja [`scripts/03_integrate_users.py`](./scripts/03_integrate_users.py). Kui jätkad pärast lisaülesannet 1, muuda lisaks faili [`scripts/lisa_01_prepare_preferences.sql`](./scripts/lisa_01_prepare_preferences.sql) ja skripti [`scripts/lisa_03_integrate_users.py`](./scripts/lisa_03_integrate_users.py), et uus väli jõuaks ka `intermediate` vaatest lõpptabelisse.

### Lisaülesanne 3: märgista kasutajad, kelle kohta osa lisainfost puudub

See ülesanne eeldab, et oled teinud lisaülesande 1.

Loo lõpptabelisse väli `has_missing_additional_data`, mis on `true`, kui kasutaja kohta puudub kas staatus või teavituseelistus.

See ülesanne aitab märgata, millise kasutaja kohta ei ole kõiki täiendavaid andmeid kätte saadud.

Vihje: arvuta see väli `intermediate.user_profile_enriched` vaates ja lae see sealt edasi lõpptabelisse. Kui jätkad pärast lisaülesannet 1, siis muuda failis [`scripts/lisa_01_prepare_preferences.sql`](./scripts/lisa_01_prepare_preferences.sql) olevat vaadet, lisa väli tabelisse `analytics.user_profile` ja uuenda ka skripti [`scripts/lisa_03_integrate_users.py`](./scripts/lisa_03_integrate_users.py), et uus väli lõpptabelisse laaditaks.

### Lisaülesanne 4: tee lihtne kokkuvõttepäring

See ülesanne eeldab, et oled teinud lisaülesande 1.

Kirjuta `SQL` päring, mis näitab:

- mitu kasutajat on iga `account_status` väärtuse all;
- mitu kasutajat eelistab kanalit `email`, `sms` või `push`.

Võid teha selle otse `psql` sees või panna uude faili `scripts/05_summary_queries.sql`.

### Lisaülesanne 5: kontrolli, kas ühendusvõtmed allikate vahel päriselt sobituvad

Põhirajal näed lõpptulemusest, et kolmel kasutajal jääb `account_status` puudu. Töövoog ise ei anna aga automaatselt teada, millised võtmed jäid sobitumata.

Selles ülesandes kontrollid võtmeid siis, kui allikad on juba tabelitena `staging` kihis olemas.

See ülesanne eeldab, et oled põhiraja läbi teinud.

Tee nii:

1. veendu, et vajalikud `staging` tabelid oleksid täidetud:

- põhirajal käivita enne fail [`scripts/03_integrate_users.py`](./scripts/03_integrate_users.py)
- kui kasutad kolme allika varianti, käivita enne fail [`scripts/lisa_03_integrate_users.py`](./scripts/lisa_03_integrate_users.py)

2. ava fail [`scripts/lisa_04_check_join_keys.py`](./scripts/lisa_04_check_join_keys.py)

See skript teeb järgmise kontrolli:

- loeb võtmed otse `staging` tabelitest;
- puhastab need ühtemoodi;
- võrdleb, millised võtmed sobituvad ja millised mitte;
- prindib lühikese raporti.

Selles praktikumis käsitleme `API` allikat põhiallikana. Lõpptabelis on üks rida iga `API` kasutaja kohta ning `CSV` ja `JSON` annavad sellele reale lisavälju juurde.

Põhirajal piisab ühest võrdlusest:

- `API` ja `CSV`

Kui oled enne teinud lisaülesande 1 ja kasutad kolme allikat, tee kaks võrdlust:

- `API` ja `CSV`
- `API` ja `JSON`

`API` jääb siin põhiallikaks, mille külge teised allikad andmeid juurde annavad. Seepärast ei ole selles ülesandes vaja eraldi võrrelda `CSV` ja `JSON` võtmeid omavahel.

See on levinud töövõte siis, kui sul on üks selge põhiallikas ja teised allikad on rikastavad allikad. Sellisel juhul küsid iga lisallika kohta: kas selle võtmed sobituvad põhiallikaga piisavalt hästi?

Kui projektis ei ole üht selget põhiallikat ja mitu allikat kirjeldavad sama nähtust võrdse kaaluga, siis võib vaja minna teistsugust lähenemist. Siis kas:

- võrdled kõiki allikaid omavahel;
- või lood eraldi ühise võtmehulgaga võrdlusbaasi, mille vastu kõiki allikaid kontrollid.

Selles praktikumis me seda keerukamat varianti ei kasuta. Siin on loogiline jada järgmine: võta põhiallikas, võrdle seda ühe rikastava allikaga, lisa järgmine rikastav allikas ja võrdle jälle põhiallikaga.

3. käivita kontrollskript hosti terminalis:

```bash
docker compose exec python python //scripts/lisa_04_check_join_keys.py
```

Põhiraja oodatav tulemus on järgmine:

```text
- Võrdlus: API ja CSV
- Sobitunud e-posti võtmeid: 7
- API poolel ilma CSV vasteta: 3
  API ainult: chaim_mcdermott@dana.io
  API ainult: rey.padberg@karina.biz
  API ainult: sherwood@rosamond.me
- CSV poolel ilma API vasteta: 3
  CSV ainult: delphine@adams.us
  CSV ainult: maxiime_nienow@alicia.info
  CSV ainult: puuduv.kasutaja@example.com
```

See kontroll ei paranda kirjavigu automaatselt. Selle eesmärk on teha sobitumata võtmed nähtavaks, et saaksid need eraldi üle vaadata.

Kui kasutad kolme allika varianti, näed samas skriptis kahte järjestikust raportit: kõigepealt `API` ja `CSV` võrdlust, seejärel `API` ja `JSON` võrdlust.

Kolme allika variandis on teise võrdluse oodatav tulemus järgmine:

```text
- Võrdlus: API ja JSON
- Sobitunud e-posti võtmeid: 7
- API poolel ilma JSON vasteta: 3
  API ainult: chaim_mcdermott@dana.io
  API ainult: rey.padberg@karina.biz
  API ainult: sherwood@rosamond.me
- JSON poolel ilma API vasteta: 3
  JSON ainult: maxiime_nienow@alicia.info
  JSON ainult: moriah.stanton@virginia.edu
  JSON ainult: varia@example.com
```

### Lisaülesanne 6: loe `Parquet` snapshot SQL-iga ja seo see lõpptabeliga

`Parquet` on veerupõhine failivorming, mida kasutatakse sageli stabiilsete snapshot'ide ja vahefailide hoidmiseks.

Selles ülesandes loed etteantud `Parquet` failist ainult vajalikud väljad ja seod need tabeliga `analytics.user_profile`.

See ülesanne eeldab, et oled põhiraja läbi teinud.

Selles keskkonnas kasutame andmebaasipilti `pgduckdb`, mis võimaldab `Parquet` failist otse SQL-iga lugeda.

Tee nii:

1. ava `psql`

```bash
docker compose exec db psql -U praktikum -d praktikum
```

2. vaata, milline `Parquet` fail välja näeb:

```sql
\i /scripts/lisa_05_preview_parquet.sql
```

See skript näitab:

- esimest 5 rida failist;
- mitu rida failis kokku on;
- kuidas võtta `Parquet` failist välja ainult vajalikud tulbad.

Kui `read_parquet` ei tööta kohe, proovi samas `psql` sessioonis käsku:

```sql
SET duckdb.force_execution = true;
```

3. loo `staging` skeemi vaade, mille kaudu saad `Parquet` snapshot'i mugavalt edasi kasutada:

```sql
\i /scripts/lisa_07_load_loyalty_snapshot.sql
```

See skript loob või uuendab vaate `staging.user_loyalty_snapshot`.

Vaade loeb failist väljad `email`, `loyalty_tier`, `risk_level` ja `snapshot_date`.

Selles ülesandes kasutad vaadet, mitte eraldi PostgreSQL tabelit. Nii saad `Parquet` faili hoida loogiliselt `staging` kihis ja kasutada sama nime hilisemates päringutes.

Pane tähele, et faili kõiki veerge ei pea alati andmebaasi lugema. Sageli piisab sellest, kui võtad stabiilsest snapshot'ist ainult need väljad, mida sul päriselt vaja on.

Selles ülesandes käsitleme `Parquet` faili teise süsteemi perioodilise snapshot'ina, mitte reaalajas uueneva operatiivtabelina. Väljad nagu `loyalty_tier` ja `risk_level` võivad lähteallikas ajas muutuda, kuid selles failis kirjeldavad need seisu kuupäeva `2026-03-10` seisuga.

4. kontrolli tulemust ja seo see lõpptabeliga:

```sql
\i /scripts/lisa_08_check_loyalty_snapshot.sql
```

Oodatav tulemus on järgmine:

- `Parquet` failis on 10 rida;
- vaates `staging.user_loyalty_snapshot` on 10 rida;
- joinitud tulemus näitab kõigi 10 kasutaja kohta välju `loyalty_tier` ja `risk_level`.

Selles ülesandes ei ole vaja uut Pythoni skripti kirjutada. Kui allikas on stabiilne ja sul on vaja sealt ainult mõnda välja, võib SQL-põhine lahendus olla täiesti piisav.

## Koristamine

Kui tahad praktikumi keskkonna lõpuks peatada:

```bash
docker compose down
```

Kui tahad alustada täiesti puhtalt, eemalda ka andmemahu sisu:

```bash
docker compose down -v
```

Kui tahad ainult praktikumi skeemid andmebaasist ära kustutada, aga konteineri alles jätta:

1. ava `psql`
2. käivita:

```sql
\i /scripts/99_reset.sql
```
