# Lahendamata tööde nimekiri

**Mis see on**: iga lahtine asi ühes tabelis, tavakeeles, ilma žargoonita. Kaks lauset mure kohta ja üks
minimaalne näide. **Supervisor uuendab seda pidevalt ise** ja raporteerib omanikule **kord tunnis**.

**Viimati uuendatud**: 2026-09-02 19:51 · **Viimati raporteeritud**: 2026-09-02 18:36

---

## 1. Vead, mis annavad vale tulemuse

| # | Mure | Näide | Seis |
|---|---|---|---|
| KI-55 | Kui panna kahe töösammu vahele teisendus, rakendatakse seda mõnes SDK-s kaks korda. Viga ei kuvata kusagil — lihtsalt vale väärtus liigub edasi. | Reegel "lisa ette `M:`" annab `M:M:mid:v`, kuigi peaks andma `M:mid:v`. | **Lahtine.** Tõsine |
| KI-144 | Kui üks kanal saadab korraga nii lisandmoodulisse kui ka järgmisse töösammu, kodeeritakse järgmise sammu sõnum vale kodeerijaga. Sõnum ei jõua kohale, proovitakse kolm korda ja pannakse kõrvale. | Töösamm ootab arvu `42`, saab 4 baiti `00 00 00 2A` ja ei oska seda lugeda. | **Parandatud ja kommititud** `6612f8bc` — meie enda viga, leitud oma checkpointil |
| KI-141 | Kui üks töösamm saadab tulemuse kahele järgmisele, siis teine neist visatakse vaikselt minema. Puudutab just seda teed, kus andmed peaks kettale jõudma. | Samm A saadab nii B-le kui C-le; C ei saa kunagi midagi. | **Parandatud ja tõestatud** — e2e jookseb neljal keelel, mõlemal transpordil |
| KI-57 | Täisrunni ajal lähevad ühendusfailide teed operatsioonisüsteemi piirist pikemaks. Siis ei saa tööline üldse ühendust ja kõik, mis teda vajas, jääb vaikselt tegemata. | Tee `/tmp/flezha_.../stream_...sock` ületab 107 tähemärki ja ühendus lihtsalt ei teki. | **Lahtine.** Tõsine |
| KI-142 | Kui kirjutada teisendus, mis ei lõpe kuskil, siis see visatakse vaikselt ära. Kaks peamist SDK-d keelavad selle, veneerid mitte. | `.map(f).sink('s')` ilma `.into()`-ta — `f` ei jookse kunagi. | Lahtine. **Üks PHP test kasutas seda päriselt** — parandatud, aga keeldu ikka pole |
| KI-143 | Pythoni üks testiseadistus toetub nimetamisreeglile, mille äsjane parandus ära võttis. Selles keeles ei jõua edastatud sõnum enam kohale. | Üks rida `fixtures.py`-s; teised kolm keelt kirjutavad selle juba õigesti. | **Lahendatud** `47d32c9d` |
| KI-129 | Elav olekupäring võib vastata olekuga, mis pole veel kettale kirjutatud, ega oska seda öelda. Andmeid ei kao, aga vastus lubab rohkem kui ta teab. | Küsid oleku, saad väärtuse, aga kettal seda veel pole. | Lahtine, väike |

## 2. Kohad, kus SDK-d käituvad erinevalt

| # | Mure | Näide | Seis |
|---|---|---|---|
| KI-121 | Nimetamata väljundkanal saab ühes SDK-s ühe nime ja teises teise. Sama programm annab eri keeltes eri tulemuse. | Rust nimetab kanali tootja järgi, veneerid väljundi järgi. | Lahtine |
| KI-118 | Mõned SDK-d lubavad ehitada topoloogia, mille Rust keelab ära kui katkise. Viga avastatakse alles hiljem või mitte kunagi. | Kuju, mille Rust keeldub kompileerimast, läheb Node'is läbi. | Lahtine |
| KI-124 | Pythoni testikomplekt nimetab kaks sammu vale nimega. Ükski test ei vaata neid nimesid, nii et miski ei lähe punaseks. | Nimi `stateful_counter` seal, kus peaks olema `persistence_off`. | Lahtine, kosmeetiline |

## 3. Asjad, mis peaks olema nähtavad, aga ei ole

| # | Mure | Näide | Seis |
|---|---|---|---|
| KI-79 | Viis käsurea käsku ütlevad "õnnestus" ka siis, kui päring päriselt ebaõnnestus. Skript ei saa vahet teha, kas asi tehti ära või mitte. | `flz ...` lõpetab koodiga 0, kuigi serveri poolelt tuli viga. | Lahtine |
| KI-80 | Kaks eri viisi sama asja küsimiseks, kumbki eirab teist vaikselt. Kasutaja saab vale kujuga vastuse ilma hoiatuseta. | `cluster status --json` ei arvesta `--output json`-it ja vastupidi. | Lahtine, oma töörida |
| KI-72 | QUIC-ühendus ei anna endast mingit statistikat. Kui midagi kinni jookseb, ei näe keegi, kus. | Kõik loendurid näitavad tühja, ka siis kui liiklus käib. | Lahtine |
| KI-115 | Kaks spetsi lubavad käitumist, mida koodis ei ole. Ühel juhul pole teada, kumb pool on vale — spets või kood. | Spets nõuab sündmust, mida `grep` leiab koodist null korda. | Lahtine, oma töörida |

## 4. Testid ja tööriistad, mis eksitavad

| # | Mure | Näide | Seis |
|---|---|---|---|
| KI-114 | Üks test ei saa põhimõtteliselt läbi minna, kui teda jooksutada koos teistega. Kui ta katki läheb, jätab ta elava protsessi taha. | Iga täisrun raporteerib `1 invalid` ja jätab deemoni käima. | Lahtine |
| KI-109 | Üks jagatud loend on kogu masina peale ühine, aga test võrdleb seda täpse võrdusega. Läheb punaseks umbes igal teisel täisrunnil ilma et midagi katki oleks. | Test ootab täpselt kahte kirjet, saab kolm, sest naaber kirjutas ka. | Lahtine |
| KI-68 | Üks töövoo test kulutab pärast kukkumist terve minuti ooteaega. Aeglustab iga täisrunni ilma midagi tõestamata. | 60 sekundit ootamist seal, kus midagi enam ei tule. | Lahtine |
| KI-117 | Üks test nõuab, et operatsioon mahuks ühte millisekundisse. See on 30 000 korda rangem kui päris nõue ja läheb koormuse all punaseks. | Piir 1 ms seal, kus leping lubab 30 sekundit. | Lahtine, väike |
| KI-132 | PHP-l on kaks töörežiimi, aga ükski test ei jooksuta teist neist otsast lõpuni. Me ei tea, kas see töötab. | Seadistus lubab `swoole` režiimi, ükski run ei kasuta seda. | Lahtine |
| KI-145 | PHP tööline võib jääda käima ka siis, kui tema töökeskkond kaob. Neli teist keelt lõpetavad ise, PHP mitte — aga põhjus pole veel leitud. | Kolm korda kolmest jäi üks PHP protsess 75 sekundiks alles; teistes keeltes kadus 2 sekundiga. | **Kahtlus** — test parandatud, tööline diagnoosimata |
| KI-133 | Ühes PHP testis on mälu, mis peaks olema jagatud, aga ei pruugi olla. Võib anda juhusliku tulemuse, kui töölisi on rohkem kui üks. | Kaks töölist, kumbki näeb oma koopiat samast lipust. | Kahtlus, pole kinnitatud |

## 5. Suured tööread, mis on veel ees

| # | Töö | Mida see tähendab | Seis |
|---|---|---|---|
| R7 | Viimane võimekus — töövood — puudub viies SDK-s. Kaheksa muudatust: neli veneeri saavad ühise paranduse, siis igaüks oma testid, siis kolm Elixiri jaoks. | Ilma selleta ei saa neis keeltes töövoogu üldse kirjutada. | **1/8 VALMIS ja arhiveeritud.** Järgmine: muudatus 2 (node'i töövoo-fikstuurid) |
| R9 | Käsurea lippude ühtlustamine (KI-80). | Väike, kokku lepitud. | Ootab |
| R10 | Avaldamise-eelne kontrollnimekiri. | Väike, ainult raamistik. | Ootab |
| R11 | Kaks spetsi, mis lubavad koodis puuduvat (KI-115). | Enne parandust tuleb mõõta, kumb pool on vale. | Ootab |
| R12 | Dokumentatsiooni viidete kontroll. | Ehitatakse viimasena, et see roheliseks jääks. | Ootab |
| — | Umbes 20 väikest järeltööd: surnud kood, abitekstide read, üks lipp mis kaob, üks mõõdik mis ei näe jälgimata faile. | Ükski pole suur; ainult proposal on kirjutamata. | Ootab |

## 6. Sinu otsust ootavad

| # | Küsimus | Miks sinu | Minu soovitus |
|---|---|---|---|
| A | Kas teha enne edasist parandamist üks läbivaatuse ring, kus keegi kontrollib iga lahtise kirje tänase koodi vastu? | Maksab paar tundi ja koodi ei tekita. | **Jah.** Üheteistkümnest tõsisest kirjest oli neli juba parandatud — nimekiri liialdab |
| B | Kas uuendada mõõdiku baasjoon nüüd või töörea lõpus? | Kirjutamine vajab sinu luba. | Töörea lõpus; praegu on vana võrdlus rangem, nii et midagi ei jää varju |
| C | `.gitignore` reegel, mis pidi arhiveeritud kavandid alles hoidma, ei tööta. Kas parandada, kustutada või jätta? | Kaotad muidu kavandid, kui see masin kaob. | Kustutada surnud reegel; hoida ainult siis, kui tahad kavandeid säilitada |

## 7. Võlgu, aga blokeeritud

| # | Mis | Miks blokeeritud |
|---|---|---|
| — | 11 muudatuse indeksirida on kirjutamata. | Teine sessioon hoiab neid kahte faili muutmata kujul; nende töö kommitimine pole minu teha |
| — | Üks dokumendiparandus ei mahu committi. | Kogu kaust on git'i eest peidetud `.git/info/exclude` reegliga |
