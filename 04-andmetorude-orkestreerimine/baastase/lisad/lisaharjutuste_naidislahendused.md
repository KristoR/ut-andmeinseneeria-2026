# Lisaharjutuste näidislahendused

Selles failis on iga lisaharjutuse kohta üks võimalik mõttekäik.

Need ei ole ametlikud ainuõiged vastused. Sama eesmärgini võib jõuda ka teist teed pidi.

Siinsed näited on kontseptsioonide ja printsiipide tasemel. Me ei muuda siin praktikumi põhikoodi ega lisa valmis uusi skripte.

## 1. Kvaliteedikontroll enne `analytics` kihti

### Eesmärk

Kontrollida enne valitud päeva koondridade uuesti ehitamist, kas tellimuste võtmed sobituvad dimensioonidega.

### Üks võimalik mõttekäik

Enne `analytics` kihi sammu võiks töövoog teha lihtsa kontrolli:

- kas kõik `orders_raw.product_id` väärtused leiduvad tabelis `staging.products_raw`;
- kas kõik `orders_raw.store_id` väärtused leiduvad tabelis `staging.stores_raw`.

Kui mõni võti ei sobitu, siis võiks töövoog:

- kirjutada selle kohta rea `staging.pipeline_run_log` tabelisse;
- katkestada jooksu arusaadava veateatega;
- jätta `analytics` kihi selle päeva jaoks uuesti ehitamata.

See on sisult sama mõte, mida nägid eelmises praktikumis võtmete sobivuse kontrolli juures. Erinevus on selles, et nüüd teed kontrolli otse orkestreeritud töövoo sees enne järgmisse kihti liikumist.

### Miks see on hea mõte?

See on parem kui vigaste andmete vaikne edasi lubamine. Kui katkine võti lasta märkamatult lõpptabelisse, on viga hiljem raskem leida ja parandada.

## 2. Igakuine dimensioonide värskendus

### Eesmärk

Hoida dimensioonid ajakohased ka siis, kui igapäevane tellimuste toru keskendub peamiselt päeva müügisündmustele.

### Uus mõiste

Aeglaselt muutuv dimensioon ehk `slowly changing dimension` kirjeldab seda, kuidas hoiame dimensioonitabelis alles muutusi ajas.

### Üks võimalik mõttekäik

Dimensioonide värskendus võiks joosta eraldi `cron` reaga, mitte sama käsu sees, mis töötleb iga päev tellimusi.

Üks võimalik ajastus on:

```text
15 3 1 * * cd /app && /usr/local/bin/python /app/scripts/orchestrate.py refresh-dimensions >> /var/log/praktikum/pipeline.log 2>&1
```

See tähendaks: iga kuu 1. päeval kell 03:15 käivita ainult dimensioonide värskendus.

Praegune praktikum käitub sisuliselt nagu tüüp 1 aeglaselt muutuv dimensioon.

See tähendab, et kui sama `product_id` või `store_id` väärtusega kirje tuleb uuesti, siis kirjutatakse olemasolevad väljad uute väärtustega üle. Me ei hoia vanemat versiooni eraldi alles.

See sobib kokku ka praeguse tehnilise lahendusega:

- tabelites `staging.products_raw` ja `staging.stores_raw` on primaarvõtmeks ärivõti;
- laadimine kasutab `ON CONFLICT ... DO UPDATE` loogikat;
- seetõttu jääb ühe ärivõtme kohta alles üks hetkeversioon.

Kui tahaksime liikuda tüüp 2 suunas, siis ainult igakuisest värskendusest ei piisaks.

Siis tuleks tavaliselt:

- võtta kasutusele surrogaatvõti ehk eraldi tehniline võti dimensioonikirje versiooni jaoks;
- hoida ärivõti eraldi väljana;
- lisada väljad, mis näitavad, millal versioon kehtib;
- vältida olemasoleva dimensioonirea pimedat ülekirjutamist;
- siduda faktid õige dimensiooniversiooniga, mitte ainult ärivõtmega.

### Miks see on hea mõte?

- inventuuri või muu perioodilise ülevaatuse järel võivad just dimensioonid vajada värskendust;
- eraldi töö ei sega päevast tellimuste toru;
- kui dimensioonid muutuvad harvemini, ei pea neid tingimata iga päev eraldi uuendama.

### Mille peale veel mõelda?

- kas sinu kasutusjuht vajab ainult viimast teadaolevat väärtust või ka muutuste ajalugu;
- millal piisab tüüp 1 lähenemisest;
- millal muutub oluliseks tüüp 2 ja ajalooline jälgitavus.

## 3. Interaktiivne veateavitus

### Eesmärk

Märgata vigu ka siis, kui keegi parajasti logifaili ise ei vaata.

### Uus mõiste

`Webhook` on automaatne `HTTP` päring, millega üks süsteem saadab teisele süsteemile sündmuse kohta teate.

### Üks võimalik mõttekäik

Kui töövoog lõpeb ka pärast `retry` katseid veaga, siis võiks ta saata teavituse näiteks:

- e-kirjaga;
- Slacki sõnumina;
- `webhook`-ina mõnda teise süsteemi.

Hea teavitus võiks sisaldada vähemalt:

- `run_id` väärtust;
- töövoo sammu nime;
- loogilist kuupäeva;
- lühikest veateadet;
- viidet sellele, kust vaadata logifaili või logitabelit.

### Mille peale veel mõelda?

- kas teavitus tuleb saata igal ebaõnnestunud katsel või alles siis, kui kõik katsed on läbi;
- kuidas vältida sama vea korduvat topeltteavitust;
- kes peaks selle teate päriselt kätte saama.

## 4. Skeemimuutuste idempotentne haldus

### Eesmärk

Muuta tabeleid nii, et töövoog jääks ka muudatuse ajal võimalikult stabiilseks.

### Uus mõiste

Migratsioon on kontrollitud andmebaasi skeemimuutus, mida rakendatakse eraldi sammuna, mitte ainult alglaadimise kõrvalmõjuna.

### Üks võimalik mõttekäik

Kui tahad lisada näiteks uue tulba, siis ainult `init/01_create_objects.sql` faili muutmisest ei piisa. See fail aitab uue andmebaasi loomisel, aga olemasolev andmemaht ei loe seda faili automaatselt uuesti sisse.

Turvaline järjekord võiks olla selline:

1. tee skeemimuutus eraldi migratsioonina;
2. uuenda laadijat nii, et ta oskab uut välja vajadusel täita;
3. uuenda `intermediate` ja `analytics` kihti, kui uus väli peab sinna edasi jõudma;
4. kontrolli tulemust;
5. vajadusel tee `backfill` või ehita mõjutatud päevad uuesti üle.

### Võimalikud näited

- `ALTER TABLE ... ADD COLUMN IF NOT EXISTS ...`
- eraldi migratsioonifail
- migratsioonilogi või skeemiversiooni tabel

### Mis jääb `run-scheduled` käsu teha ja mis mitte?

`run-scheduled` oskab selles praktikumis ainult tavapärast päevatöötlust:

- ta leiab järgmise puuduva valmis päeva või töötleb aktiivset äripäeva uuesti;
- ta loeb tellimused API-st;
- ta kirjutab need `staging` kihti;
- ta ehitab valitud päeva koondread `analytics` kihti uuesti.

Ta ei tee sinu eest skeemimuudatust.

Kui skeem muutub, siis peab kasutaja või eraldi juurutussamm vähemalt ühe korra ise tegema näiteks järgmised asjad:

- rakendama migratsiooni;
- uuendama laadijat;
- vajadusel uuendama `intermediate` ja `analytics` kihti;
- otsustama, kas vanad päevad tuleb uuesti üle ehitada.

Alles pärast seda saab `run-scheduled` jätkata tavapärast päevade töötlemist.

### Mis juhtub siis, kui alustad kõik uuesti algusest?

Kui teed `docker compose down -v`, siis alustad puhta andmebaasiga.

Sellisel juhul:

- uus skeem tuleb `init/01_create_objects.sql` failist;
- vanad `staging` ja `analytics` andmed kaovad;
- dimensioonid tuleb uuesti laadida;
- tellimuste ajalugu tuleb uuesti API-st või muudest allikatest sisse tuua.

See tee on lihtne, aga eeldab, et vajalik ajalugu on allikas endiselt olemas.

### Mis siis, kui `analytics` kihis on andmeid, mida API enam ei anna?

Selles praktikumis ehitatakse `analytics` ümber tabeli `staging.orders_raw` põhjal, mitte otse API vastusest.

See tähendab:

- kui olemasolev andmebaas jääb alles ja vanad tellimused on endiselt `staging.orders_raw` tabelis olemas, siis saab nende päevade `analytics` read uuesti ehitada ka siis, kui sa sama päeva API-st enam uuesti ei päri;
- kui alustad kõik nullist ja API seda ajalugu enam ei paku, siis ei saa neid ridu enam taastada ainult selle praktikumi töövooga;
- kui valid skeemimuutuse järel ainult tulevaste päevade töötlemise, siis võivad vanad `analytics` read jääda mõneks ajaks vana kujuga, kuni otsustad need päevad uuesti üle ehitada.

Praeguse praktikumi kohaliku API puhul aktiivse päeva sündmused päeva jooksul ainult lisanduvad. Ta ei võta varem nähtud tellimusi vastusest ära.

Päriselus võib allikas mõnikord ka varasemaid ridu parandada või eemaldada. Siis oleks vaja eraldi strateegiat, näiteks kustutuste arvestust, snapshot'e või muud muutuste ajalugu.

### Miks idempotentsus on siin oluline?

Kui sama migratsiooni sammu või skeemimuutust on vaja uuesti rakendada, siis ei tohiks see tekitada topeltmuudatusi ega lõhkuda juba töötavat töövoogu. Just sellepärast kasutataksegi sageli tingimuslikke skeemimuutusi ja eraldi migratsioonide arvestust.
