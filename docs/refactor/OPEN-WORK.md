# Lahendamata tööde nimekiri

**Mis see on**: iga lahtine asi ühes tabelis, tavakeeles, ilma žargoonita. Kaks lauset mure kohta ja üks
minimaalne näide. **Supervisor uuendab seda pidevalt ise** ja raporteerib omanikule **kord tunnis**.

**Viimati uuendatud**: 2026-09-02 23:48 · **Viimati raporteeritud**: 2026-09-02 23:48

---

## 1. Vead, mis annavad vale tulemuse

| # | Mure | Näide | Seis |
|---|---|---|---|
| KI-55 | Kui panna kahe töösammu vahele teisendus, rakendatakse seda mõnes SDK-s kaks korda. Viga ei kuvata kusagil — lihtsalt vale väärtus liigub edasi. | Reegel "lisa ette `M:`" annab `M:M:mid:v`, kuigi peaks andma `M:mid:v`. | **Lahtine.** Tõsine |
| KI-57 | Täisrunni ajal lähevad ühendusfailide teed operatsioonisüsteemi piirist pikemaks. Siis ei saa tööline üldse ühendust ja kõik, mis teda vajas, jääb vaikselt tegemata. | Tee `/tmp/flezha_.../stream_...sock` ületab 107 tähemärki ja ühendus lihtsalt ei teki. | **Lahtine.** Tõsine |
| KI-142 | Kui kirjutada teisendus, mis ei lõpe kuskil, siis see visatakse vaikselt ära. Kaks peamist SDK-d keelavad selle, veneerid mitte. | `.map(f).sink('s')` ilma `.into()`-ta — `f` ei jookse kunagi. | Lahtine. **Üks PHP test kasutas seda päriselt** — parandatud, aga keeldu ikka pole |

## 2. Kohad, kus SDK-d käituvad erinevalt

| # | Mure | Näide | Seis |
|---|---|---|---|

## 3. Asjad, mis peaks olema nähtavad, aga ei ole

| # | Mure | Näide | Seis |
|---|---|---|---|
| KI-148 | Üks käsurea käsk annab `--output json`-iga hoopis teise nimekirja, mitte lihtsalt teise kuju. Skript, mis küsib paigaldatud pluginaid, saab hoopis pakkujate registri. | `plugin list --json` ja `plugin list --output json` teevad kaks eri päringut. | Uus, läheb R9 sisse koos KI-80-ga |
| KI-80 | Kaks eri viisi sama asja küsimiseks, kumbki eirab teist vaikselt. Kasutaja saab vale kujuga vastuse ilma hoiatuseta. | `cluster status --json` ei arvesta `--output json`-it ja vastupidi. | Lahtine, oma töörida |
| KI-72 | QUIC-ühendus ei anna endast mingit statistikat. Kui midagi kinni jookseb, ei näe keegi, kus. | Kõik loendurid näitavad tühja, ka siis kui liiklus käib. | Lahtine, **alandatud** — vaikimine on kinni, ainult mõõdikud puudu |
| KI-115 | Kaks spetsi lubavad käitumist, mida koodis ei ole. Ühel juhul pole teada, kumb pool on vale — spets või kood. | Spets nõuab sündmust, mida `grep` leiab koodist null korda. | Lahtine, oma töörida |

## 4. Testid ja tööriistad, mis eksitavad

| # | Mure | Näide | Seis |
|---|---|---|---|
| KI-68 | Üks töövoo test kulutab pärast kukkumist terve minuti ooteaega. Aeglustab iga täisrunni ilma midagi tõestamata. | 60 sekundit ootamist seal, kus midagi enam ei tule. | **ESKALEERITUD — sinu otsus.** Põhjus leitud ja mõõdetud; iga parandus muudaks toodangu käitumist |
| KI-132 | PHP-l on kaks töörežiimi, aga ükski test ei jooksuta teist neist otsast lõpuni. Me ei tea, kas see töötab. | Seadistus lubab `swoole` režiimi, ükski run ei kasuta seda. | Lahtine |
| KI-145 | PHP tööline võib jääda käima ka siis, kui tema töökeskkond kaob. Neli teist keelt lõpetavad ise, PHP mitte — aga põhjus pole veel leitud. | Kolm korda kolmest jäi üks PHP protsess 75 sekundiks alles; teistes keeltes kadus 2 sekundiga. | **Kahtlus** — test parandatud, tööline diagnoosimata |

## 5. Suured tööread, mis on veel ees

| # | Töö | Mida see tähendab | Seis |
|---|---|---|---|
| R7 | Viimane võimekus — töövood — puudub viies SDK-s. Kaheksa muudatust: neli veneeri saavad ühise paranduse, siis igaüks oma testid, siis kolm Elixiri jaoks. | Ilma selleta ei saa neis keeltes töövoogu üldse kirjutada. | **5/8 VALMIS. Kõik kuus FFI-keelt kannavad töövooge.** Elixiri pool (6–8) on sinu, kavand kirjutatud |
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

| D | KI-68: üks test kulutab 60 s ootamist igal täisrunnil. Kas muuta toodangu ootamist selle pärast? | Iga võimalik parandus muudab toodangu käitumist, mitte ainult testi. | **Ei praegu.** Põhjus on leitud ja kirjas; 60 s ühe testi kohta ei õigusta äravoolu-loogika muutmist enne reedet |

## 7. Võlgu, aga blokeeritud

| # | Mis | Miks blokeeritud |
|---|---|---|
| — | **12** muudatuse indeksirida on kirjutamata. | Teine sessioon hoiab neid kahte faili muutmata kujul; nende töö kommitimine pole minu teha |
| — | Üks dokumendiparandus ei mahu committi. | Kogu kaust on git'i eest peidetud `.git/info/exclude` reegliga |

## 8. Selle sessiooni jooksul lahendatud

| # | Mis | Kus |
|---|---|---|
| KI-114 | Test ei saanud kunagi koos teistega läbi minna ja jättis protsessi taha. | `983787a9` — deemon nõudis sama olekukausta, mida test ise juba hoidis |
| KI-79 | Viis käsurea käsku pidid andma vale õnnestumiskoodi. | **Ümber lükatud** — koodid on õiged, midagi ei olnud parandada |
| KI-118 | SDK-d pidid lubama kuju, mille Rust keelab. | **Ümber lükatud** — kõik neli keelavad selle nüüd |
| KI-121 | Nimetamata kanal pidi saama eri keeltes eri nime. | **Ümber lükatud** — `55b8a748` lahendas selle seda nimetamata |
| KI-129 | Elav olekupäring võis vastata kirjutamata olekuga. | **Lahendatud** — testi võidujooks kadus, semantika jäi nagu otsustatud |
| KI-141 | Hargnemise teine haru kadus vaikselt kettateel. | Mudel `55b8a748`, **tõestatud traadil neljas keeles** koos negatiivse kontrolliga |
| KI-143 | Pythoni fikstuur nimetas kanali valesti. | `47d32c9d` |
| KI-144 | Hübriidkanal kodeeris järgmise sammu vale kodeerijaga, iga kirje DLQ-sse. | `6612f8bc` — meie enda viga, leitud oma checkpointil |
| KI-146 | `setup_all`-is käivitatud tööline oli alati Rusti oma. | `f670106f` |
| KI-147 | Kõik neli veneeri saatsid töövoo kohta välja seadistuse, mida tuum keeldub. | `cc1763c3` — **neli testi kinnitasid viga paigale** |
| KI-124 | Pythoni fikstuur nimetas kaks sammu nagu tema kaksik. | `b61889f0` |
| KI-117 | Test nõudis 1 ms seal, kus leping lubab 30 s. | `b61889f0` |
| KI-109 | Jagatud loend võrreldud täpse võrdusega + leke, mis selle põhjustas. | `b61889f0` |
| KI-133 | PHP jagatud mälu kahtlus — **ümber lükatud**, koodi ei kirjutatud. | `b61889f0` |
