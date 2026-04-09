# Praktikum 4: Andmetorude orkestreerimine kohaliku `cron`-töövooga

## Sisukord

- [Praktikumi eesmärk](#praktikumi-eesmärk)
- [Õpiväljundid](#õpiväljundid)
- [Hinnanguline ajakulu](#hinnanguline-ajakulu)
- [Eeldused](#eeldused)
- [Enne alustamist](#enne-alustamist)
- [Praktikumi failid](#praktikumi-failid)
- [Kus praktikumi failid asuvad?](#kus-praktikumi-failid-asuvad)
- [Miks see teema on oluline?](#miks-see-teema-on-oluline)
- [Miks siin `cron`, mitte Airflow?](#miks-siin-cron-mitte-airflow)
- [Uued mõisted](#uued-mõisted)
- [ETL etapid selles praktikumis](#etl-etapid-selles-praktikumis)
- [Soovitatud töötee](#soovitatud-töötee)
- [1. Ava praktikumi kaust](#1-ava-praktikumi-kaust)
- [2. Vaata üle praktikumi teenused ja failid](#2-vaata-ule-praktikumi-teenused-ja-failid)
- [3. Käivita keskkond](#3-kaivita-keskkond)
- [4. Kontrolli, et teenused töötavad](#4-kontrolli-et-teenused-tootavad)
- [5. Vaata üle andmeallikad](#5-vaata-ule-andmeallikad)
- [6. Kontrolli, et andmebaasi objektid on loodud](#6-kontrolli-et-andmebaasi-objektid-on-loodud)
- [7. Käivita töövoog käsitsi ühe päeva jaoks](#7-kaivita-toovoog-kasitsi-uhe-paeva-jaoks)
- [8. Kontrolli tulemusi `SQL`-iga](#8-kontrolli-tulemusi-sql-iga)
- [9. Kontrolli idempotentsust](#9-kontrolli-idempotentsust)
- [10. Proovi `retry` loogikat](#10-proovi-retry-loogikat)
- [11. Käivita `backfill`](#11-kaivita-backfill)
- [12. Vaata inkrementaalset `run-scheduled` loogikat](#12-vaata-inkrementaalset-run-scheduled-loogikat)
- [13. Uuri `cron`-i seadistust ja logifaili](#13-uuri-cron-i-seadistust-ja-logifaili)
- [Kontrollpunktid](#kontrollpunktid)
- [Levinud vead ja lahendused](#levinud-vead-ja-lahendused)
- [Kokkuvõte](#kokkuvote)
- [Valikuline lisaharjutus](#valikuline-lisaharjutus)
- [Koristamine](#koristamine)

## Praktikumi eesmärk

Selle praktikumi eesmärk on ehitada väike, aga päriselt töötav andmetoru, mis käivitub `cron`-ajastusega.

Me ei kasuta siin välist `API`-t. Selle asemel töötab praktikumi sees kohalik allikas, mis simuleerib väikese e-poe päevaseid tellimusi. Toote- ja poeloendid tulevad `CSV`-failidest. Tellimused tulevad kohalikust `HTTP API`-st ehk veebipäringutele vastavast liidesest.

Praktikumi lõpuks näed läbi terve tööahela:

- kuidas `cron` käivitab töö automaatselt;
- kuidas töövoog kirjutab logisid logifaili ja logitabelisse;
- kuidas sama päeva saab turvaliselt uuesti käivitada;
- kuidas `retry`, `backfill` ja inkrementaalne töörežiim käituvad.

## Õpiväljundid

Praktikumi lõpuks oskad:

- käivitada `docker compose` abil väikese orkestreerimise praktikumi keskkonna;
- selgitada, mida teeb `cron`-avaldis `*/5 * * * *`;
- värskendada dimensioone eraldi käsuga;
- käivitada töövoogu käsitsi etteantud loogilise kuupäevaga;
- kontrollida logifailist ja logitabelist, mis sammus toru parajasti on;
- kirjeldada, miks `retry`, `backfill` ja idempotentsus on ajastatud tööde puhul vajalikud;
- jälgida andmete liikumist kihtides `staging -> intermediate -> analytics`.

## Hinnanguline ajakulu

Arvesta umbes 2 kuni 2,5 tunniga.

See aeg jaguneb ligikaudu nii:

- 20 min keskkonna käivitamiseks ja failidega tutvumiseks;
- 20 min allikate ja skeemide läbivaatamiseks;
- 25 min käsikäivituse ja tulemuse kontrolli jaoks;
- 20 min idempotentsuse ja `retry` näite jaoks;
- 20 min `backfill` ja inkrementaalse töörežiimi jaoks;
- 15 min `cron`-i logifaili ja tõrkeotsingu jaoks.

Kui tahad kõik käsud mitu korda läbi proovida ja tulemusi rahulikult võrrelda, arvesta pigem 2,5 tunniga.

## Eeldused

Sul on vaja:

- `VS Code`-i või GitHub Codespacesit;
- terminali;
- töötavat Dockeri keskkonda;
- selle repositooriumi faile.

Kasuks tuleb, kui eelmiste praktikute põhjal on tuttavad järgmised töövõtted:

- oskad liikuda õige praktikumi kausta;
- tead, mis vahe on hosti terminalil ja konteineri sees töötaval käsul;
- oskad avada `psql` kliendi käsuga `docker compose exec scheduler psql ...`;
- tead, et andmetoru võib töödelda ühte loogilist päeva ka siis, kui päris kellaaeg on teine.

Kui mõni neist kohtadest on veel ebakindel, vaata vajadusel üle:

- [Praktikum 1: PostgreSQL-iga ühenduse loomine ja esimese CSV-faili laadimine](../../01-andmeinseneeria-alused/baastase/README.md)
- [Praktikum 3: Andmete integreerimine API ja CSV abil](../../03-andmete-integreerimine/baastase/README.md)

## Enne alustamist

### Soovitatud keskkond

Selle praktikumi jaoks sobib hästi järgmine tööviis:

- ava kaust `04-andmetorude-orkestreerimine/baastase` `VS Code`-is;
- kasuta `VS Code`-i sisseehitatud terminali;
- hoia ühes aknas lahti `README.md`, teises `compose.yml` ja `scripts/orchestrate.py`;
- käivita käsud hosti terminalist, kui juhendis ei ole öeldud teisiti.

Kui töötad Windowsis, siis arvesta kahe asjaga:

- Bashi näidetes kasutame kujusid `export MUUTUJA=...` ja `$MUUTUJA`;
- PowerShellis on samad kujundid vastavalt `$env:MUUTUJA = "..."` ja `$env:MUUTUJA`.

PowerShelli erivormid on lisatud sinna, kus need erinevad Bashi näidetest.

Kui töötad GitHub Codespacesis, siis on praktikumi kaust tavaliselt siin:

```text
/workspaces/ut-andmeinseneeria-2026/04-andmetorude-orkestreerimine/baastase
```

### Puhas algus

See praktikum kasutab järgmisi hosti porte:

- andmebaas `5435`;
- kohalik allika-`API` `8014`.

Kui need pordid on mõne muu teenuse poolt hõivatud, peata konfliktne teenus enne praktikumi alustamist.

Kui oled selle praktikumi juba varem käivitanud ja tahad täiesti puhast algust, kasuta juhendi lõpus olevat käsku:

```bash
docker compose down -v
```

See eemaldab ka andmebaasi mahuühenduse. Järgmisel käivitamisel luuakse skeemid ja tabelid uuesti.

## Praktikumi failid

Kõik allpool toodud suhtelised failiteed eeldavad, et asud kaustas `04-andmetorude-orkestreerimine/baastase`.

- [`compose.yml`](./compose.yml) kirjeldab kolme teenust: andmebaasi, kohalikku `API`-t ja `cron`-ajastajat ehk `scheduler` teenust
- [`.env.example`](./.env.example) sisaldab praktikumi vaikimisi keskkonnamuutujaid
- [`Dockerfile.scheduler`](./Dockerfile.scheduler) ehitab scheduler'i konteineri, kuhu paigaldatakse `cron`, `psql` klient ja Pythoni teegid
- [`init/01_create_objects.sql`](./init/01_create_objects.sql) loob skeemid, tabelid ja `intermediate` vaate, kui andmebaas esimest korda initsialiseeritakse
- [`source_api/server.py`](./source_api/server.py) käivitab kohaliku tellimuste `API`
- [`source_data/products.csv`](./source_data/products.csv) sisaldab tootedimensiooni lähteandmeid
- [`source_data/stores.csv`](./source_data/stores.csv) sisaldab poedimensiooni lähteandmeid
- [`scripts/orchestrate.py`](./scripts/orchestrate.py) on praktikumi keskne orkestreerija
- [`scripts/01_check_pipeline.sql`](./scripts/01_check_pipeline.sql) sisaldab kontrollpäringuid
- [`scripts/requirements.txt`](./scripts/requirements.txt) loetleb scheduler'i Pythoni sõltuvused
- [`scheduler/crontab`](./scheduler/crontab) määrab `cron`-i ajastuse ja käivitatava käsu
- [`scheduler/entrypoint.sh`](./scheduler/entrypoint.sh) paigaldab `crontab` faili ja käivitab `cron` teenuse
- [`lisad/lisaharjutuste_naidislahendused.md`](./lisad/lisaharjutuste_naidislahendused.md) koondab lisaharjutuste ühe võimaliku näidismõtte
- [`logs/.gitkeep`](./logs/.gitkeep) hoiab logide kausta repos olemas

## Kus praktikumi failid asuvad?

Selles praktikumis on korraga kasutusel neli konteksti:

- host ehk sinu arvuti või Codespace;
- andmebaasi konteiner `db`;
- scheduler'i konteiner `scheduler`;
- kohaliku allika konteiner `source-api`.

Sama fail võib olla eri kontekstides eri teega.

Näited:

- hostis on fail `source_data/products.csv`;
- scheduler'i konteineris on sama fail `/app/source_data/products.csv`;
- hostis on fail `scripts/01_check_pipeline.sql`;
- scheduler'i konteineris on sama fail `/app/scripts/01_check_pipeline.sql`.

See vahe on oluline, sest:

- `cron` käivitab käsu scheduler'i konteineri sees;
- selles juhendis avame `psql` kliendi scheduler'i konteineris, mitte andmebaasi konteineris;
- see meenutab rohkem päris tööolukorda, kus töövoog või rakendus ühendub andmebaasi üle võrgu;
- `psql -f scripts/01_check_pipeline.sql` loeb faili scheduler'i konteineri vaatest;
- `\copy ... FROM 'source_data/products.csv'` loeb faili samuti scheduler'i konteineri vaatest, sest `\copy` on `psql` kliendi käsk;
- `COPY ... FROM '/tee/fail.csv'` loeb faili aga andmebaasiserveri vaatest ja see on teistsugune olukord;
- kohalik tellimuste `API` töötab konteineri sees aadressil `http://source-api:8014`, aga hostist pääsed sellele ligi aadressil `http://localhost:8014`.

Kui kasutad Windowsis `Git Bash`-i ja annad `docker compose exec` käsule absoluutse konteineritee, siis võib `Git Bash` algava kaldkriipsu oma moodi tõlgendada.
Selle vältimiseks kasutamegi selles juhendis enamasti suhtelisi teid, näiteks `scripts/orchestrate.py` ja `scripts/01_check_pipeline.sql`.
Kui sul on siiski vaja anda absoluutne konteineritee, siis kirjuta see kujul `//app/...` või `//var/log/...`.
Logifaili vaatamise käskudes kasutame sellepärast kuju `sh -lc '...'`, et absoluutne tee jääks konteineri shelli sisse.

Kui kasutaksid siin `\copy` käsku, siis eelista teed `source_data/...`.
Absoluutne tee `/app/source_data/...` töötab samuti, kuid `Git Bash`-is võib olla vaja kuju `//app/source_data/...`.

## Miks see teema on oluline?

Tööelus ei piisa sellest, et üks `ETL` skript oskab käsitsi käivitades andmed õigesse kohta laadida.

Vaja on ka vastuseid järgmistele küsimustele:

- millal töö käivitub;
- mis juhtub, kui allikas on ajutiselt maas;
- kuidas näha, kas töö õnnestus;
- kuidas laadida puudu jäänud ajalugu tagantjärele;
- kuidas vältida seda, et korduskäivitus tekitab duplikaate.

Selles praktikumis keskendume just sellele kihile. Andmetorust saab töövoog, mida saab ajastada, jälgida ja vajadusel turvaliselt uuesti käivitada.

## Miks siin `cron`, mitte Airflow?

Selles praktikumis on töövoog väike ja lineaarne:

1. lae dimensioonid, kui vaja;
2. päri tellimused;
3. lae need `staging` kihti;
4. ehita valitud päeva koondread `analytics` kihti uuesti.

Selle jaoks sobib `cron` hästi.

`cron` on lihtne ja juba Linuxis olemas. See aitab näha ajastuse põhiloogikat ilma suurema orkestreerija lisakihtideta.

Edasijõudnute rajal liigume Airflow peale siis, kui vaja on:

- rikkamat sõltuvusgraafi;
- paremat nähtavust veebiliideses;
- rohkem harusid ja paralleelsust;
- eraldi orkestreerimissüsteemi.

## Uued mõisted

Selle jaotise käsunäited on mõistete selgitamiseks.
Me ei eelda, et proovid neid kõiki kohe terminalis läbi.
Päris praktilised käsud algavad sammust 1.

### `cron`

Probleem on lihtne: skript töötab, aga keegi peaks seda õigel ajal käivitama.

`cron` on Linuxi sisseehitatud ajastaja. Tema ülesanne on käivitada käske kindla ajamustri järgi.

Nimi `cron` ei ole siin akronüüm. Tavaliselt kirjutame selle väiketähtedega.
Ääremärkusena tasub teada, et nime taustaks tuuakse tavaliselt kreeka sõna `chronos`, mis seostub ajaga.

Selles praktikumis käivitab `cron` scheduler'i konteineri sees käsu:

See plokk on näide, mitte eraldi läbiproovitav samm.

```text
python scripts/orchestrate.py run-scheduled
```

### `cron`-avaldis

Kui tahame öelda, *millal* käsk käivitub, on vaja kokkulepitud kuju.

`cron`-avaldis on viiest osast koosnev ajamuster.

Allolev lühike näiteplokk aitab meeles pidada, mida iga positsioon tähendab:
See plokk on lugemiseks ja meeldejätmiseks.

```text
# Positsioonid vasakult paremale:
# minut   tund   päev kuus   kuu   nädalapäev
# -----   ----   --------   ---   ----------
#
# Sümbolid:
# *       = iga võimalik väärtus
# */2     = iga 2 ühiku tagant
# 1-5     = vahemik 1 kuni 5
# 1,3,5   = loetelu ehk mitu eraldi väärtust

*/2 * * * *     # iga 2 minuti tagant
10-59/2 * * * * # alates minutist 10 iga 2 minuti tagant
0 */3 * * *     # iga 3 tunni järel täistunnil
5 6 * * *       # iga päev kell 06:05
15 9 * * 1-5    # tööpäeviti kell 09:15
0 8 1 * *       # iga kuu 1. päeval kell 08:00
0 8 * * 1,3,5   # esmaspäeval, kolmapäeval ja reedel kell 08:00
```

Oluline tähelepanek märgi `/` kohta:

- `*/2` tähendab "võta kõik võimalikud väärtused ja liigu neist üle kahe";
- minuti väljas tähendab see iga kahe minuti tagant;
- tunni väljas tähendaks sama kuju iga kahe tunni tagant.

### Loogiline kuupäev

Ajastatud töö puhul ei taha me alati töödelda “praegust hetke”.

Loogiline kuupäev ütleb, millise päeva andmeid töövoog käsitleb.

Näiteks:

See käsk on kuju näitamiseks. Päris käsikäivituse teeme sammus 7.

```bash
docker compose exec scheduler python scripts/orchestrate.py run-once --logical-date YYYY-MM-DD
```

See käivitab toru etteantud loogilise kuupäeva jaoks isegi siis, kui päris kuupäev või kellaaeg on juba teine.

### Idempotentsus

Ajastatud töö peab taluma korduskäivitust.

Idempotentsus tähendab, et sama töö korduv käivitamine annab sama tulemuse, mitte uusi duplikaate.

Selles praktikumis on see nähtav kahel tasemel:

- `staging.orders_raw` kasutab `UPSERT`-i. See tähendab: kui rida on juba olemas, siis uuendame seda, mitte ei lisa teist samasugust rida juurde;
- `analytics.daily_product_sales` ehitatakse valitud päeva jaoks iga kord uuesti nullist.

### `Retry`

Vahel ebaõnnestub `API` päring ajutiselt. See ei tähenda veel, et kogu töövoog oleks lootusetult katki.

`Retry` tähendab, et ebaõnnestunud sammu proovitakse uuesti.

Selles praktikumis teeb `retry` loogika orkestreerija ise. Scheduler'i `cron`-rida ei tea midagi vigadest ega katsetest. Tema käivitab lihtsalt töö. Tarkus on skriptis.

### `Backfill`

Kui mõni päev jäi töötlemata, on vaja see tagantjärele ära teha.

`Backfill` tähendabki möödunud kuupäevade töötlemist.

Näites:

See käsk on kuju näitamiseks. Päris `backfill`-i teeme sammus 11.

```bash
docker compose exec scheduler python scripts/orchestrate.py backfill --from-date YYYY-MM-DD --to-date YYYY-MM-DD
```

### Bootstrap

Enne kui töövoog saab päriselt jooksma hakata, on vaja minimaalset algseadistust.

`Bootstrap` tähendabki süsteemi esmast ettevalmistust nii, et ülejäänud osad saaksid pärast selle najal tööle minna.

Selles praktikumis tähendab see, et andmebaas loob esimesel käivitamisel vajalikud skeemid, tabelid ja vaate faili `init/01_create_objects.sql` põhjal.

Mõnikord kohtad seda sõna ka kujul “andmebaasi bootstrap” või “töövoo bootstrap”.

Ääremärkusena on sõna taustaks tuntud väljend “to pull oneself up by one’s bootstraps”. Algselt oli see pigem võimatu enese üles tõmbamise kujund. Arvutimaailmas hakati selle all hiljem mõistma väikest alglaadimist (booting), millest ülejäänud süsteem käima läheb.

### Inkrementaalne töörežiim

Kui kõiki päevi pole vaja igal käivitusel uuesti töödelda, on mõistlik eristada kahte olukorda:

- varasemad päevad, mida peame juba valmis päevadeks;
- tänane äripäev, mille andmed võivad veel muutuda.

Selles praktikumis teeb seda käsk `run-scheduled`.

Ta töötab kahes järgus:

- kõigepealt otsib järgmise puudu jäänud valmis päeva;
- kui kõik valmis päevad on tehtud, töötleb ta aktiivset äripäeva uuesti üle.

Valmisolekut hindame selle järgi, kas `build_analytics` samm õnnestus. Ainult `load_orders` edust ei piisa.

### Logifail ja logitabel

Töövoo jälgimiseks on vaja vähemalt kahte vaadet:

- logifail, mis näitab käsu väljundit jooksvas järjekorras;
- logitabel, mis võimaldab hiljem samm-sammult tulemust pärida.

Selles praktikumis:

- logifail asub scheduler'i konteineris failis `/var/log/praktikum/pipeline.log`;
- logitabel on tabel `staging.pipeline_run_log`.

Selles praktikumis on seire passiivne. Meie vaatame logifaili ja logitabelit ise. Töövoog ei saada veel ise interaktiivset teavitust ehk automaatset e-kirja, Slacki sõnumit või muud teadet, kui töövoog lõpeb veaga.

## ETL etapid selles praktikumis

Selles praktikumis hoiame nähtaval kihtide rolli.

1. `staging`
   Dimensioonid laaditakse võimalikult allikalähedaselt tabelitesse `products_raw` ja `stores_raw`.
   Tellimused laaditakse võimalikult allikalähedaselt tabelisse `orders_raw`.

2. `intermediate`
   Vaade `intermediate.orders_enriched` seob tellimused toodete ja poodidega ning arvutab rea kogusumma.

3. `analytics`
   Tabel `analytics.daily_product_sales` hoiab iga töödeldud päeva koondridu päeva, poe ja toote lõikes.

See on kooskõlas põhimõttega, et kiht kirjeldab andmete rolli, mitte tehnoloogiat.
Käsk `refresh-dimensions` jääb ainult `staging` kihti. Käsud `run-once`, `backfill` ja `run-scheduled` liiguvad sealt edasi `intermediate` ja `analytics` kihtidesse.

## Soovitatud töötee

Selle praktikumi mõistmiseks on hea liikuda järgmises järjekorras:

1. käivita teenused;
2. vaata üle allikad;
3. tee üks käsikäivitus;
4. kontrolli tulemust `SQL`-iga;
5. tee sama päeva korduskäivitus;
6. proovi `retry` loogikat;
7. tee `backfill`;
8. vaata, kuidas `run-scheduled` ja `cron` käituvad.

Nii tuleb üks uus idee korraga.

## 1. Ava praktikumi kaust

See samm tehakse hosti terminalis.

Kui alustad repo juurkaustast, siis liigu praktikumi kausta:

```bash
cd 04-andmetorude-orkestreerimine/baastase
```

## 2. Vaata üle praktikumi teenused ja failid

See samm tehakse hostis.

Enne kui teenused käivitame, loo fail `.env`.
`docker compose` loeb praktikumi muudetavad väärtused sellest failist.
Repositooriumis on olemas ainult näidisfail `.env.example`.

macOS-is, Linuxis või Git Bashis:

```bash
cp .env.example .env
```

Windows PowerShellis:

```powershell
Copy-Item .env.example .env
```

Praegu ei ole vaja seal väärtusi muuta.
Fail `.env` on lisatud `.gitignore`-i, seega see ei lähe kogemata repositooriumisse.

Ava fail `compose.yml` ja vaata üle kolm teenust:

- `db` hoiab andmebaasi;
- `source-api` simuleerib päevaseid tellimusi;
- `scheduler` käivitab `cron`-i ja läbi selle regulaarselt ka orkestreerija skripti.

Vaata korraks ka järgmisi faile:

- `source_api/server.py`
- `scripts/orchestrate.py`
- `scheduler/crontab`

Sa ei pea veel kõigest aru saama. Eesmärk on näha, et praktikumi tuum koosneb ühest ajastajast ja ühest töövooskriptist.

Pane tähele ka seda, et `compose.yml` loeb muudetavad väärtused failist `.env`.
Seetõttu on `compose.yml` ise lühem ja teenusekirjeldused on lihtsamini loetavad.

## 3. Käivita keskkond

See samm tehakse hosti terminalis.

See käsk eeldab, et eelmises sammus loodud fail `.env` on olemas.

Käivita teenused:

```bash
docker compose up -d --build
```

Esimesel käivitamisel kulub rohkem aega, sest scheduler'i konteiner ehitatakse `Dockerfile.scheduler` põhjal.
Esimesel andmebaasi käivitamisel tehakse ka andmebaasi algseadistus ehk bootstrap. `compose.yml` mountib kausta `init` andmebaasi konteinerisse teele `/docker-entrypoint-initdb.d` ja andmebaas loeb sealt faili `01_create_objects.sql`.
Selle tulemusel luuakse skeemid `staging`, `intermediate` ja `analytics`, vajalikud tabelid ning vaade `intermediate.orders_enriched`.
Kui andmebaasi maht jääb alles, siis seda algseadistust automaatselt uuesti ei tehta.

Oodatav tulemus:

- andmebaasi konteiner käivitub;
- kohalik `API` käivitub pordil `8014`;
- scheduler käivitab `cron` teenuse taustal.

Oluline tähelepanek: kui scheduler on käivitunud, siis hakkab `cron` oma vaikeloogikat kohe taustal jooksutama.
See tähendab, et selleks ajaks, kui jõuad sammu 7, võivad dimensioonid olla juba laaditud, mitu valmis päeva juba töödeldud ja `analytics` kihis võib olla rohkem kui ühe päeva koondridu.
See on selles praktikumis normaalne. Samm 7 ei eelda täiesti tühja algseisu, vaid aitab näha, kuidas üks käsitsi käivitatud töövoog käitub olemasoleva seisu peal.

## 4. Kontrolli, et teenused töötavad

See samm tehakse hostis. Kasutad siin nii terminali kui ka brauserit.

Kontrolli teenuste olekut:

```bash
docker compose ps
```

Oodatav tulemus: `praktikum-db-04-base`, `praktikum-source-api-04-base` ja `praktikum-scheduler-04-base` on olekus `running` või `healthy`.

Kontrolli kohalikku `API`-t brauseris.

Ava aadress:

```text
http://localhost:8014/docs
```

Kui töötad Codespacesis, siis ava sama aadress edasi suunatud pordi `8014` kaudu.

Oodatav tulemus:

- avaneb brauseris lihtne dokumentatsioonileht;
- näed seal allika kuupäevavahemikku ja aktiivset äripäeva;
- lehel on lingid `GET /health` ja `GET /api/orders` päringute proovimiseks.

Sea samas hosti terminalis kaks abimuutujat, et järgmistes käskudes ei peaks kuupäevi käsitsi kirjutama.
Allolev näide eeldab `.env.example` vaikeväärtusi.
Kui muutsid `.env` failis kuupäevi, kohanda ka need read.

macOS-is, Linuxis või Git Bashis:

```bash
export SOURCE_START=2026-04-01
export PREVIOUS_DATE=2026-04-08
```

Windows PowerShellis:

```powershell
$env:SOURCE_START = "2026-04-01"
$env:PREVIOUS_DATE = "2026-04-08"
```

Need muutujad tähendavad:

- `SOURCE_START` on allika esimene saadaval olev kuupäev;
- `PREVIOUS_DATE` on viimane kuupäev, mida võib juba lõpetatuks pidada.

Kui kasutad samu käske hiljem samas terminaliaknas, jäävad muutujad meelde.
Kui kasutad PowerShelli, siis kirjuta hilisemates käskudes `$SOURCE_START` asemel `$env:SOURCE_START` ja `$PREVIOUS_DATE` asemel `$env:PREVIOUS_DATE`.

## 5. Vaata üle andmeallikad

See samm tehakse hostis.

Praktikum kasutab kahte tüüpi allikat.

### Staatilised ekspordid

Failid:

- `source_data/products.csv`
- `source_data/stores.csv`

Need kirjeldavad dimensioone. Need muutuvad harva. Sellepärast ei ole mõistlik neid iga käivitusega tingimata uuesti laadida.
Kui vaja, saame neid värskendada eraldi käsuga `refresh-dimensions`.

### Kohalik tellimuste `API`

Brauseris loetav dokumentatsioon asub aadressil:

```text
http://localhost:8014/docs
```

### Lühidokumentatsioon

- `GET /health`
  Tagastab teenuse oleku ja kuupäevakonteksti.
  Vastusest näed, mis kuupäevast alates ja kuni andmeid pakutakse, milline päev on aktiivne äripäev ja millise kuupäevani peetakse andmeid juba lõpetatuks.

- `GET /api/orders?date=YYYY-MM-DD&mode=stable|fail_once`
  Tagastab ühe päeva tellimused.
  Parameeter `date` määrab loogilise kuupäeva.
  Parameeter `mode=stable` annab tavapärase vastuse.
  Parameeter `mode=fail_once` on mõeldud `retry` proovimiseks.

Tellimuste päringu kuju on:

See plokk näitab päringu kuju. Kõige mugavam on proovida seda brauseri kaudu lehelt `/docs`.

```text
GET /api/orders?date=YYYY-MM-DD&mode=stable|fail_once
```

Proovi docs lehelt linki `Valmis päeva tellimused` või ava brauseris `GET /health` link.

Oodatav tulemus:

- `health` vastus tuleb `JSON`-vormingus ehk struktureeritud tekstina;
- `orders` vastuses on väljad nagu `date`, `business_date`, `is_final`, `order_count` ja `orders`;
- aktiivse äripäeva ja valmis päeva vastused võivad erineda, sest aktiivne päev võib veel muutuda.

Pane tähele ka seda:

- valmis päevade puhul annab sama kuupäev alati sama tellimuste komplekti;
- päevane sündmuste arv võib kuupäeviti veidi erineda, kuid jääb tavaliselt umbes saja ümber;
- aktiivse äripäeva puhul sõltub vastus ka kellaajast;
- aktiivse äripäeva tellimused koonduvad rohkem õhtusse, umbes kella 18 ümbrusse, nii et praktikumi ajal näed tavaliselt uusi sündmusi juurde tulemas;
- sama kuupäeva tulemus on siiski deterministlik ehk korduvatel päringutel sama, sest näiline juhuslikkus tuleb fikseeritud seemne loogikast;
- `mode=fail_once` on mõeldud `retry` proovimiseks;
- homse või veel hilisema kuupäeva kohta API vastust ei anna;
- andmeallikas töötab ainult vahemikus, mida näed `health` vastusest.

Selles praktikumis on aktiivne äripäev fikseeritud.
See ei sõltu sinu arvuti päris tänasest kuupäevast.
Seepärast käitub `run-scheduled` pärast ajalooliste päevade täitmist nii, nagu päris töövoog käituks "täna" jooksul: ta töötleb sama päeva uuesti üle, mitte ei loe seda kohe lõplikult valmis.

## 6. Kontrolli, et andmebaasi objektid on loodud

See samm tehakse hosti terminalis, aga `psql` jookseb scheduler'i konteineris.

Scheduler'i konteineris on `psql` kliendi ühendusemuutujad juba ette seadistatud. Seepärast piisab siin käsust `psql ...`.
Lipp `-c` tähendab, et `psql` käivitab ühe etteantud käsu ja lõpetab siis töö.

Vaata skeemid üle:

```bash
docker compose exec scheduler psql -c "\dn"
```

Oodatav tulemus: näed skeeme `staging`, `intermediate` ja `analytics`.

Vaata tabelid ja vaade üle:

```bash
docker compose exec scheduler psql -c "\dt staging.*"
docker compose exec scheduler psql -c "\dt analytics.*"
docker compose exec scheduler psql -c "\dv intermediate.*"
```

Oodatav tulemus:

- `staging.products_raw`
- `staging.stores_raw`
- `staging.orders_raw`
- `staging.pipeline_run_log`
- `analytics.daily_product_sales`
- `intermediate.orders_enriched`

Kui `cron` on jõudnud juba mõne korra taustal käivituda, siis võivad mõned neist tabelitest selleks hetkeks ka ridu sisaldada. See on oodatav.

Väike modelleerimise märkus:
Selles näites kasutame dimensioonides ärivõtit ehk allikast tulevat tunnust `product_id` ja `store_id` otse võtmena.
Surrogaatvõti tähendab süsteemi enda loodud tehnilist võtit, näiteks `product_key`.
Päris projektides kasutatakse sageli surrogaatvõtit, kuid selle praktikumi väikeses näites hoiab ärivõti seosed lihtsamini jälgitavana.

## 7. Käivita töövoog käsitsi ühe päeva jaoks

See samm tehakse hosti terminalis, aga töö jookseb scheduler'i konteineris.

Kõigepealt värskenda dimensioonid eraldi.
See käsk ei tee `API` päringut ega ehita `analytics` kihti.
Ta uuendab ainult tabelid `staging.products_raw` ja `staging.stores_raw`.
Nii on ETL töövoogu lihtsam jälgida: kõigepealt maandame dimensioonid `staging` kihti ja seejärel töötleme ühe päeva tellimused edasi kuni `analytics` kihini.
See käsk töötab samal kujul ka Windows PowerShellis.

```bash
docker compose exec scheduler python scripts/orchestrate.py refresh-dimensions
```

Oodatav tulemus:

- terminalis näed `load_products` ja `load_stores` logiridu;
- `staging.pipeline_run_log` tabelisse tekivad nende sammude kohta `success` olekuga read;
- see käsk ei lisa uusi tellimusi ega ehita ise `analytics` kihti, kuigi varasemad read võivad seal juba olemas olla, kui `cron` on enne seda jooksnud.

Seejärel käivita viimase valmis päeva töövoog:

macOS-is, Linuxis või Git Bashis:

```bash
docker compose exec scheduler python scripts/orchestrate.py run-once --logical-date "$PREVIOUS_DATE"
```

Windows PowerShellis:

```powershell
docker compose exec scheduler python scripts/orchestrate.py run-once --logical-date "$env:PREVIOUS_DATE"
```

See käsk teeb kolm põhiasja:

1. kontrollib, kas dimensioonid on olemas;
2. pärib kohalikust `API`-st tellimused ja laeb need `staging.orders_raw` tabelisse;
3. ehitab `analytics.daily_product_sales` tabelisse selle päeva koondread uuesti.

Oluline täpsustus: siin ei arvutata koondtulemust üle kõigi laetud päevade korraga.
Iga käivitus kustutab ja ehitab uuesti ainult selle loogilise kuupäeva read, mida parajasti töötleme.
See aitab hoida töövoo idempotentsena: sama päeva võib turvaliselt uuesti käivitada, ilma et koondread hakkaksid duplitseeruma.

Kui tegid eelmise käsu juba ära, siis jäetakse dimensioonide laadimine siin vahele ja see logitakse olekuga `skipped`.

Oodatav tulemus:

- terminalis näed sammude logiridu;
- `staging.pipeline_run_log` tabelisse tekivad `success` olekuga read;
- valitud päeva koondread ehitatakse `analytics` kihis uuesti;
- `analytics` kihis võivad selleks hetkeks olla juba ka teiste päevade koondread, kui `cron` on need vahepeal ära töödelnud.

## 8. Kontrolli tulemusi `SQL`-iga

See samm tehakse hosti terminalis, aga `SQL` jookseb scheduler'i konteineris.

Käivita valmis kontrollpäringute fail:

```bash
docker compose exec scheduler psql -f scripts/01_check_pipeline.sql
```

See fail näitab:

- mitu rida on toote- ja poedimensioonis;
- mitu tellimust on iga päeva kohta;
- milline näeb välja lõpptabel;
- millised logiread viimati tekkisid.

Vaata eraldi ka `intermediate` vaadet:

```bash
docker compose exec scheduler psql -c "SELECT order_date, store_name, product_name, quantity, total_amount_eur FROM intermediate.orders_enriched ORDER BY order_date, order_id LIMIT 10;"
```

See kontroll aitab näha, et `intermediate` vaate roll ei ole ainult “vahekiht”. Siin toimub rikastamine ja rea kogusumma arvutus.

## 9. Kontrolli idempotentsust

See samm tehakse hosti terminalis.

Käivita sama päev uuesti:

macOS-is, Linuxis või Git Bashis:

```bash
docker compose exec scheduler python scripts/orchestrate.py run-once --logical-date "$PREVIOUS_DATE"
```

Windows PowerShellis:

```powershell
docker compose exec scheduler python scripts/orchestrate.py run-once --logical-date "$env:PREVIOUS_DATE"
```

Miks see on oluline?

Kui ajastatud töö jookseb teist korda, ei tohi tulemus “paisuda” lihtsalt sellepärast, et käivitusi oli kaks.

Kontrolli ridade arvu:

macOS-is, Linuxis või Git Bashis:

```bash
docker compose exec scheduler psql -c "SELECT COUNT(*) AS orders_for_day FROM staging.orders_raw WHERE order_date = DATE '$PREVIOUS_DATE';"
docker compose exec scheduler psql -c "SELECT COUNT(*) AS analytics_rows_for_day FROM analytics.daily_product_sales WHERE sales_date = DATE '$PREVIOUS_DATE';"
```

Windows PowerShellis:

```powershell
docker compose exec scheduler psql -c "SELECT COUNT(*) AS orders_for_day FROM staging.orders_raw WHERE order_date = DATE '$env:PREVIOUS_DATE';"
docker compose exec scheduler psql -c "SELECT COUNT(*) AS analytics_rows_for_day FROM analytics.daily_product_sales WHERE sales_date = DATE '$env:PREVIOUS_DATE';"
```

Oodatav tulemus: ridade arv ei tohi korduskäivituse tõttu kasvada.

Põhjus on järgmine:

- `staging.orders_raw` kasutab `UPSERT`-i ehk olemasoleva rea uuendamist sama võtme korral;
- `analytics.daily_product_sales` kustutab sama päeva read enne uuesti ehitamist.

## 10. Proovi `retry` loogikat

See samm tehakse hosti terminalis.

Nüüd simuleerime ajutist `API` viga.

Käivita sama päev nii, et allikas ebaõnnestub esimesel katsel:

macOS-is, Linuxis või Git Bashis:

```bash
docker compose exec scheduler python scripts/orchestrate.py run-once --logical-date "$PREVIOUS_DATE" --source-mode fail_once
```

Windows PowerShellis:

```powershell
docker compose exec scheduler python scripts/orchestrate.py run-once --logical-date "$env:PREVIOUS_DATE" --source-mode fail_once
```

Oodatav käitumine:

- esimene `extract_orders` katse ebaõnnestub;
- orkestreerija ootab lühidalt;
- järgmine katse õnnestub;
- kogu töövoog lõpeb `success` olekuga.

Vaata logitabeli ridu:

macOS-is, Linuxis või Git Bashis:

```bash
docker compose exec scheduler psql -c "SELECT step_name, logical_date, attempt_no, status, message FROM staging.pipeline_run_log WHERE logical_date = DATE '$PREVIOUS_DATE' ORDER BY id DESC LIMIT 10;"
```

Windows PowerShellis:

```powershell
docker compose exec scheduler psql -c "SELECT step_name, logical_date, attempt_no, status, message FROM staging.pipeline_run_log WHERE logical_date = DATE '$env:PREVIOUS_DATE' ORDER BY id DESC LIMIT 10;"
```

Mida tähele panna:

- `attempt_no` kasvab;
- vähemalt üks `extract_orders` rida on olekuga `error`;
- viimane sama sammu rida on olekuga `success`.

## 11. Käivita `backfill`

See samm tehakse hosti terminalis.

Lae tagantjärele kõik valmis päevad alates allika algusest:

macOS-is, Linuxis või Git Bashis:

```bash
docker compose exec scheduler python scripts/orchestrate.py backfill --from-date "$SOURCE_START" --to-date "$PREVIOUS_DATE"
```

Windows PowerShellis:

```powershell
docker compose exec scheduler python scripts/orchestrate.py backfill --from-date "$env:SOURCE_START" --to-date "$env:PREVIOUS_DATE"
```

See käsk töötleb vahemiku päeva haaval.

Kontrolli tulemust:

```bash
docker compose exec scheduler psql -c "SELECT order_date, COUNT(*) AS orders_rows FROM staging.orders_raw GROUP BY order_date ORDER BY order_date;"
docker compose exec scheduler psql -c "SELECT sales_date, COUNT(*) AS analytics_rows FROM analytics.daily_product_sales GROUP BY sales_date ORDER BY sales_date;"
```

Käivita sama `backfill` veel kord:

macOS-is, Linuxis või Git Bashis:

```bash
docker compose exec scheduler python scripts/orchestrate.py backfill --from-date "$SOURCE_START" --to-date "$PREVIOUS_DATE"
```

Windows PowerShellis:

```powershell
docker compose exec scheduler python scripts/orchestrate.py backfill --from-date "$env:SOURCE_START" --to-date "$env:PREVIOUS_DATE"
```

Oodatav tulemus: ridade arv ei tohi suureneda ainult sellepärast, et tegid sama `backfill` käsu uuesti.

## 12. Vaata inkrementaalset `run-scheduled` loogikat

See samm tehakse hosti terminalis.

`run-scheduled` vaatab, millised päevad on juba päriselt valmis.

See tähendab siin kahte asja:

- varasem päev loetakse valmis alles siis, kui `build_analytics` samm on edukas;
- aktiivset äripäeva ei loeta lõpetatuks, vaid see kirjutatakse vajaduse korral uuesti üle.

Proovi seda käsitsi:

```bash
docker compose exec scheduler python scripts/orchestrate.py run-scheduled
```

Kui sul on valmis päevade hulgas mõni auk, siis valib see käsk järgmise puudu jäänud valmis päeva.

Kui kõik valmis päevad on juba tehtud, siis töötleb `run-scheduled` aktiivset äripäeva uuesti üle. Nii käitub see rohkem nagu päris töövoog, kus tänase päeva andmed võivad päeva jooksul veel täieneda.

See on siin teadlik valik.
Kuni praktikumi aktiivne äripäev püsib samana, jääb `cron` seda päeva uuesti töötlema. Päev muutub lõplikult valmis päevaks alles siis, kui allika "äriline tänane päev" liigub järgmisse kuupäeva.

Kontrolli viimaseid logiridu:

```bash
docker compose exec scheduler psql -c "SELECT step_name, logical_date, status, message FROM staging.pipeline_run_log ORDER BY id DESC LIMIT 10;"
```

See on selle praktikumi inkrementaalne töörežiim:

- ära töötle kõike korraga;
- täida esmalt valmis päevade augud;
- hoia aktiivne äripäev ajakohane korduskäivitustega.

## 13. Uuri `cron`-i seadistust ja logifaili

See samm tehakse hosti terminalis.

Vaata `cron`-i seadistust:

```bash
docker compose exec scheduler crontab -l
```

Sa peaksid nägema rida:

```text
*/5 * * * * cd /app && /usr/local/bin/python /app/scripts/orchestrate.py run-scheduled >> /var/log/praktikum/pipeline.log 2>&1
```

See tähendab:

- käivita töö iga viie minuti tagant;
- kirjuta väljund logifaili;
- töötle korraga kas üks puudu jäänud valmis päev või aktiivne äripäev.

Vaata logifaili:

Esimene valik on avada fail `logs/pipeline.log` otse `VS Code`-is.
See töötab, sest logifail on scheduler'i konteinerist hosti kausta mountitud.

Kui fail on veel tühi või tahad näha viimaseid ridu terminalis, kasuta:

```bash
docker compose exec scheduler sh -lc 'tail -n 50 /var/log/praktikum/pipeline.log'
```

Kui tahad näha, kuidas `cron` käitub siis, kui kõik valmis päevad on tehtud, lae kõigepealt ajalooline osa lõpuni:

macOS-is, Linuxis või Git Bashis:

```bash
docker compose exec scheduler python scripts/orchestrate.py backfill --from-date "$SOURCE_START" --to-date "$PREVIOUS_DATE"
```

Windows PowerShellis:

```powershell
docker compose exec scheduler python scripts/orchestrate.py backfill --from-date "$env:SOURCE_START" --to-date "$env:PREVIOUS_DATE"
```

Seejärel oota üks `cron`-tsükkel ja vaata logifaili uuesti:

Esimene valik on jälle avada fail `logs/pipeline.log` `VS Code`-is ja värskendada vaadet.

Kui eelistad terminali, kasuta:

```bash
docker compose exec scheduler sh -lc 'tail -n 50 /var/log/praktikum/pipeline.log'
```

Oodatav tulemus: logifailist näed, et `cron` liigub nüüd aktiivse äripäeva juurde ja töötleb selle uuesti üle.

Kui tahad seda piiri ise läbi mängida, siis muuda failis `compose.yml` väärtus `SOURCE_BUSINESS_DATE` järgmisse päeva ja käivita teenused uuesti:

```bash
docker compose down
docker compose up -d --build
```

Siis muutub ka seni aktiivne äripäev valmis päevaks.

## Kontrollpunktid

Selleks hetkeks peaksid sul olema järgmised asjad kontrollitud.

- Pärast `docker compose up -d --build` töötavad kolm teenust.
- `source-api` vastab `/health` päringule.
- Andmebaasis on skeemid `staging`, `intermediate` ja `analytics`.
- Dimensioone saab vajadusel värskendada eraldi käsuga.
- Üks käsikäivitus laeb tellimused ja ehitab valitud päeva koondread uuesti.
- Sama päeva korduskäivitus ei tekita duplikaate.
- `fail_once` režiim tekitab vähemalt ühe ebaõnnestunud katse ja seejärel eduka katse.
- `backfill` lisab puuduvaid päevi ilma ridu dubleerimata.
- `run-scheduled` leiab järgmise puudu jäänud valmis päeva ja töötleb hiljem aktiivset äripäeva uuesti üle.
- `cron`-i seadistus, logifail ja logitabel on nähtavad.

## Levinud vead ja lahendused

### Sümptom: `docker compose up` annab vea, et port `5435` või `8014` on juba kasutusel

Tõenäoline põhjus: mõni teine konteiner või kohalik teenus kasutab sama porti.

Lahendus:

- peata konfliktne konteiner või teenus;
- käivita seejärel praktikumi teenused uuesti.

### Sümptom: `psql` käsud ütlevad, et tabelit või skeemi ei leitud

Tõenäoline põhjus: andmebaas ei ole veel valmis või vanas mahuühenduses on jäänud eelmine seis.

Lahendus:

```bash
docker compose ps
docker compose down -v
docker compose up -d --build
```

See lahendus on eriti oluline siis, kui muutsid `init/01_create_objects.sql` faili pärast esimest käivitust.

### Sümptom: `run-once` ütleb, et kuupäev peab jääma kindlasse vahemikku

Tõenäoline põhjus: kohalik `API` annab andmeid ainult talle seadistatud kuupäevavahemikus.

Lahendus:

- kasuta `health` vastuse põhjal leitud kuupäevi või eelpool loodud shell-muutujaid;
- vajadusel kontrolli `health` vastusest, milline vahemik on saadaval.

Kui küsid kuupäeva, mis on aktiivsest äripäevast hilisem, siis katkestab töövoog kohe. Seda ei proovita `retry`-ga uuesti, sest tegu ei ole ajutise veaga.

### Sümptom: `retry` näide ei näita enam esimest ebaõnnestunud katset

Tõenäoline põhjus: sama katse on selle scheduler'i töökorra jooksul juba korra läbi tehtud.

Lahendus:

- kasuta selle proovimiseks teist kuupäeva samast vahemikust;
- või taaskäivita praktikumi teenused käsuga `docker compose down` ja `docker compose up -d --build`.

### Sümptom: `crontab -l` töötab, aga logifail ei muutu

Tõenäoline põhjus: scheduler ei tööta või `cron`-i järgmine käivitushetk pole veel kätte jõudnud.

Lahendus:

```bash
docker compose ps
docker compose exec scheduler pgrep cron
docker compose exec scheduler sh -lc 'tail -n 50 /var/log/praktikum/pipeline.log'
```

Kui kõik valmis päevad on juba tehtud, siis on oodatav, et `cron` liigub aktiivse äripäeva uuesti töötlemise juurde.

### Sümptom: kontrollpäringute fail ei avane käsuga `psql -f scripts/01_check_pipeline.sql`

Tõenäoline põhjus: käsk jookseb vales konteineris või failitee ei ole scheduler'i konteineri vaatest õige.

Lahendus:

- kontrolli, et kasutad käsku `docker compose exec scheduler ...`;
- kontrolli, et asud kaustas `04-andmetorude-orkestreerimine/baastase`.

## Kokkuvõte

Selles praktikumis ehitasid väikese, aga päris andmetoru, millel on olemas ajastus, logimine ja uuesti käivitamise loogika.

Oluline ei olnud ainult see, et andmed jõuaksid tabelisse.

Oluline oli ka see, et sa oskaksid vastata järgmistele küsimustele:

- mis päevale töö jooksis;
- mis sammus ta parajasti oli;
- kas ta õnnestus või ebaõnnestus;
- kas sama päeva võib uuesti töödelda;
- kuidas puudu jäänud ajalugu tagasi täita.

Kui need küsimused on kontrolli all, on sul juba esimene päris orkestreeritud töövoog olemas.

Samas on sellel baastaseme töövool kaks teadlikku piiri.

- Töövoog kogub logisid, kuid ei saada ise interaktiivseid veateavitusi.
- Töövoog eeldab stabiilset skeemi ega halda skeemimuutusi automaatselt.

Need teemad jätame järgmisteks sammudeks. Valikuliste lisaharjutuste juures saad nende üle juba edasi mõelda.

## Valikuline lisaharjutus

Lisaharjutused on iseseisvaks katsetamiseks ja mõtlemiseks. Neid ei pea esitama.

Iga harjutuse kohta on olemas üks võimalik näidismõte failis [lisad/lisaharjutuste_naidislahendused.md](./lisad/lisaharjutuste_naidislahendused.md). See ei ole ametlik ainus õige vastus, vaid üks võimalik suund.

Vali üks lisaharjutus või proovi mitut.

1. Lisa töövoole üks lihtne kvaliteedikontroll.

Mõtle, kuidas kontrollida enne `analytics` kihi ehitamist, kas kõik tellimuste `product_id` ja `store_id` väärtused leiduvad vastavates dimensioonides. Kui mõni võti ei sobitu, siis peaks töövoog selle logima ja arusaadavalt katkestama.

See mõte jätkab eelmise praktikumi võtmete sobivuse kontrolli loogikat. Võid meenutada [Praktikum 3 võtmekontrolli lisaülesannet](../../03-andmete-integreerimine/baastase/README.md#lisaulesanne-5-kontrolli-kas-uhendusvotmed-allikate-vahel-pariselt-sobituvad) ja vaadata ka faili [lisa_04_check_join_keys.py](../../03-andmete-integreerimine/baastase/scripts/lisa_04_check_join_keys.py).

Võid mõelda sellest näiteks nii:

```text
loe tellimustest kõik product_id väärtused selle päeva kohta
loe tellimustest kõik store_id väärtused selle päeva kohta

leia, millised product_id väärtused puuduvad tabelist staging.products_raw
leia, millised store_id väärtused puuduvad tabelist staging.stores_raw

kui vähemalt üks puuduv võti leidub:
    kirjuta logitabelisse veateade
    katkesta töövoog enne analytics kihi ehitamist
muidu:
    jätka analytics kihi ehitamist
```

2. Lisa `scheduler/crontab` faili teine rida, mis värskendab dimensioone kord kuus pärast inventuuri.

Mõtle, miks võiks see töö joosta eraldi igapäevasest tellimuste torust ja miks võiks kord kuus olla selles näites piisav.
Mõtle juurde ka sellele, millise aeglaselt muutuva dimensiooni ehk `slowly changing dimension` tüübi moodi see praktikum praegu käitub.

Näiteks võid kasutada sellist ajastust:

```text
15 3 1 * * cd /app && /usr/local/bin/python /app/scripts/orchestrate.py refresh-dimensions >> /var/log/praktikum/pipeline.log 2>&1
```

See rida tähendaks: iga kuu 1. päeval kell 03:15 käivita ainult dimensioonide värskendus.

3. Mõtle läbi interaktiivne veateavitus.

Interaktiivne teavitus tähendab seda, et süsteem annab inimesele ise märku, kui midagi läheb valesti. Näiteks võib see olla e-kiri, Slacki sõnum või `webhook` ehk automaatne päring teise süsteemi teavitamiseks.

Mõtle, millal peaks töövoog teavituse saatma, millist infot peaks teade sisaldama ja kuidas vältida sama vea korduvaid topeltteavitusi.

4. Mõtle läbi skeemimuutuste idempotentne haldus.

Migratsioon on kontrollitud skeemimuutus, mis viiakse andmebaasis läbi eraldi sammuna. Mõtle, kuidas lisada uus tulp või muu skeemimuutus nii, et töövoog jääks tööle ka siis, kui sama migratsiooni sammu või skeemimuutust on vaja uuesti rakendada.

Mõtle ka sellele, miks ainult `init/01_create_objects.sql` faili muutmine ei aita olemasoleva andmemahu korral, mida saab `run-scheduled` pärast muudatust veel ise teha ja milline peaks olema see osa, mille teed kasutajana ühe korra käsitsi ära.

## Koristamine

Kui tahad teenused lihtsalt peatada:

```bash
docker compose down
```

Kui tahad teenused peatada ja kõik selle praktikumi andmed eemaldada:

```bash
docker compose down -v
```

Teine käsk viib praktikumi puhtasse algseisu.
