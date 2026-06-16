# Skripte za reprodukciju rezultata (Prilog uz tehnički izveštaj)

Ovih 5 PowerShell skripti reprodukuju tačno postupak kojim su dobijene
vrednosti u tabelama (Sekcije 1, 2, 3, 5, 6 u `REZULTATI.md`). 
## Preduslovi

- Docker Desktop pokrenut
- Ceo stack već gore (`docker compose up -d --build` iz root foldera projekta)
- Skripte se pokreću **iz root foldera projekta** (gde je `docker-compose.yml`)
- PowerShell (Windows)

## Kako rade

Svaka skripta piše/prepisuje `docker-compose.override.yml` sa potrebnim
environment varijablama (`BROKER_TYPE`, `MQTT_QOS`, `KAFKA_ACKS`,
`DEVICE_COUNT`, `Broker__Mode`, itd.), pokreće
`docker compose up -d --build <servisi>` da primeni izmene, i zatim izvodi
mereni scenario (network disconnect, čekanje, brojanje redova u
`sensor_data`, Consumer Lag).

`docker-compose.override.yml` se automatski učitava od strane `docker compose`
pored glavnog `docker-compose.yml` - nema potrebe za `-f` flagovima.

## Skripte

### 01-qos-scenario-b.ps1 — QoS testiranje (MQTT) + Scenario B (Sekcija 1)

```powershell
.\01-qos-scenario-b.ps1 -QoS 0
.\01-qos-scenario-b.ps1 -QoS 1
.\01-qos-scenario-b.ps1 -QoS 2
```

Postavlja MQTT mod sa zadatim publish QoS-om (ingestion), diskonektuje
`iot-storage` na 30s, mери broj redova pre/posle i ispisuje logove (gleda se
"catch-up" skok u veličini flush batch-a).

### 02-acks-scenario-b.ps1 — acks testiranje (Kafka) + Scenario B + Consumer Lag (Sekcija 2)

```powershell
.\02-acks-scenario-b.ps1 -Acks 0
.\02-acks-scenario-b.ps1 -Acks 1
.\02-acks-scenario-b.ps1 -Acks -1
```

Isto kao gore, ali Kafka mod + Consumer Lag (`kafka-consumer-groups.sh`,
grupa `data-storage-group`) pre / tokom / posle diskonekcije.

### 03-scenario-a.ps1 — Massive Sensor Ingestion (Sekcija 3)

```powershell
.\03-scenario-a.ps1 -DeviceCount 100
.\03-scenario-a.ps1 -DeviceCount 1000
.\03-scenario-a.ps1 -DeviceCount 10000   # OPREZ - vidi napomenu ispod
```

Postavlja `DEVICE_COUNT`, Kafka mod sa `acks=1`, mери throughput i procenat
gubitka tokom 60s (parametrizovano sa `-DurationSeconds`).

> **Napomena za DEVICE_COUNT=10000:** u našem testiranju, 10000 simuliranih
> uređaja (10000 setInterval-a u jednom Node.js procesu) je dovelo
> `iot-ingestion` na >300% CPU i >2GB RAM, opterećujući ceo host (8 jezgara,
> ukupno >440% CPU). Test je morao biti prekinut pre kompletnih 60s. Ovo je
> sâmo po sebi nalaz: **granica je u single-instance simulatoru uređaja
> (Node.js event loop), ne u Kafka brokeru** — Kafka/data-storage su
> apsorbovali nagli prirast (>5000 msg/s) bez greške. Ako se skripta pokrene
> sa `-DeviceCount 10000`, preporučuje se paralelno pratiti `docker stats` i
> ručno prekinuti (`docker compose stop data-ingestion`) ako CPU/RAM ne
> stagnira u prvih 15-20s.

### 04-scenario-c.ps1 — Burst Event Load (Sekcija 5)

```powershell
.\04-scenario-c.ps1                                    # default 100 -> 2000 -> 100
.\04-scenario-c.ps1 -BaselineDevices 100 -BurstDevices 2000
```

Tri faze (baseline 30s, burst ~35s, recovery 4x20s), mери Consumer Lag u
svakoj fazi — pokazuje formiranje i drenažu backlog-a.

### 05-scenario-d.ps1 — E2E Alerting Latency (Sekcija 6)

```powershell
.\05-scenario-d.ps1
.\05-scenario-d.ps1 -DurationSeconds 60
```

Prati `iot-analytics` logove i izdvaja "Prozor [10s]" i "CRITICAL ALERT"
linije sa timestamp-ovima — latencija = razlika timestamp-ova (u našem
testu, ~0s, isti sekundni timestamp).

## Resetovanje nakon testiranja

Nakon završenih testova, da vratiš sistem na "default" konfiguraciju iz
glavnog `docker-compose.yml`:

```powershell
Remove-Item docker-compose.override.yml
docker compose up -d --build
```
