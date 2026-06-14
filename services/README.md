# Data Storage Service (.NET)

ASP.NET Core Worker Service koji se pretplaćuje na broker (MQTT ili Kafka,
birano preko `Broker__Mode`) i upisuje primljene senzorske podatke u
PostgreSQL (`sensor_data` tabela iz `db/init.sql`).

## Struktura

```
data-storage/
├── DataStorage.csproj
├── Program.cs                  -> DI setup, izbor MQTT/Kafka konzumera
├── Worker.cs                   -> batching logika (500 poruka ili 5s)
├── appsettings.json            -> default (lokalna) konfiguracija
├── appsettings.Development.json-> konfiguracija za lokalno pokretanje (localhost)
├── Dockerfile
├── Models/SensorReading.cs     -> mapira JSON payload sa ingestion servisa
├── Consumers/
│   ├── IMessageConsumer.cs     -> interfejs (MQTT/Kafka swap)
│   ├── MqttConsumer.cs         -> MQTTnet implementacija
│   └── KafkaConsumer.cs        -> Confluent.Kafka implementacija
└── Data/SensorDataRepository.cs-> bulk insert preko Postgres COPY (binary)
```

## Kako radi

1. `MqttConsumer` se konektuje na `mqtt://<host>:1883`, pretplaćuje se na
   topic `iot/sensors` sa konfigurabilnim QoS-om (0/1/2) i `CleanSession=false`
   (bitno za Scenario B – broker čuva poruke za vreme diskonekcije kod QoS 1/2).
2. Svaka primljena poruka se deserijalizuje u `SensorReading` i ubacuje u
   in-memory `Channel`.
3. `Worker` čita iz channel-a i puni bafer. Bafer se flush-uje (bulk insert
   preko `COPY ... FROM STDIN BINARY`) kada:
   - dostigne `Batching:BatchSize` (default 500), ili
   - prođe `Batching:FlushIntervalSeconds` (default 5s) od poslednjeg flush-a.
4. Ako se postavi `Batching:DisableWrites=true`, podaci se samo broje i
   odbacuju – korisno za Scenario A/C ako I/O postane bottleneck.
5. `KafkaConsumer` (kada je `Broker__Mode=kafka`) se konektuje preko
   Confluent.Kafka na `Broker__Kafka__BootstrapServers`, koristi sopstvenu
   consumer group (`data-storage-group`, odvojeno od `analytics-group`) tako
   da Consumer Lag za naš servis može da se prati nezavisno:
   ```bash
   docker exec -it iot-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
     --bootstrap-server localhost:9092 --describe --group data-storage-group
   ```

## Pokretanje lokalno (bez Dockera)

Potreban .NET 8 SDK.

```bash
cd services/data-storage
dotnet restore
dotnet run
```

Po defaultu (Development) koristi `localhost:1883` za MQTT i
`localhost:5432` za Postgres – pokreni samo te dve usluge iz
docker-compose-a:

```bash
docker compose up postgres mqtt
```

## Konfiguracija preko environment varijabli

Sve vrednosti iz `appsettings.json` mogu se override-ovati env varijablama
(ASP.NET Core convention, `:` -> `__`):

| Env var                          | Default              | Opis                          |
|-----------------------------------|----------------------|-------------------------------|
| `Broker__Mode`                    | `mqtt`               | `mqtt` ili `kafka`             |
| `Broker__Mqtt__Host`              | `localhost`          | hostname MQTT brokera          |
| `Broker__Mqtt__Port`              | `1883`               | port                            |
| `Broker__Mqtt__Topic`             | `iot/sensors`        | topic                           |
| `Broker__Mqtt__QoS`               | `1`                  | 0, 1 ili 2 – za QoS testiranje  |
| `Broker__Kafka__BootstrapServers` | `localhost:9092`     | adresa Kafka brokera            |
| `Broker__Kafka__Topic`            | `iot/sensors`        | Kafka topic                     |
| `Broker__Kafka__GroupId`          | `data-storage-group` | consumer group (za Consumer Lag)|
| `Broker__Kafka__AutoOffsetReset`  | `earliest`           | `earliest` ili `latest`         |
| `Database__ConnectionString`      | (lokalni postgres)   | Npgsql connection string       |
| `Batching__BatchSize`             | `500`                | prag za bulk insert            |
| `Batching__FlushIntervalSeconds`  | `5`                  | max vreme do flush-a            |
| `Batching__DisableWrites`         | `false`              | true = ne upisuje u bazu         |

To znači da za **QoS testiranje (Obojica)** samo treba pokrenuti servis tri
puta sa `Broker__Mqtt__QoS=0`, `=1`, `=2` i meriti latenciju/gubitak poruka.

## Prebacivanje na Kafka mod

U `docker-compose.yml`, servis `data-storage` trenutno ima samo MQTT env
varijable. Za testiranje Kafka moda, override-uj sledeće (npr. u
`docker-compose.override.yml` ili direktno za vreme testa):

```yaml
  data-storage:
    environment:
      Broker__Mode: kafka
      Broker__Kafka__BootstrapServers: iot-kafka:9092
      Broker__Kafka__Topic: iot/sensors
      Broker__Kafka__GroupId: data-storage-group
      Broker__Kafka__AutoOffsetReset: earliest
```

> **Napomena:** Kafka topic imena ne mogu sadržati `/` (dozvoljeni karakteri:
> `a-zA-Z0-9._-`). `iot/sensors` će vrlo verovatno baciti `InvalidTopicException`
> kada se prvi put pokuša subscribe/produce na pravi Kafka broker. Ako se to
> desi, topic treba promeniti (npr. u `iot.sensors`) na sva tri mesta:
> `data-ingestion`, `analytics` i ovde (`Broker__Kafka__Topic`).

## docker-compose.yml

Servis `data-storage` (container `iot-storage`) je već u glavnom
`docker-compose.yml`, sa MQTT modom kao default-om:

```yaml
  data-storage:
    build:
      context: ./services/data-storage
      dockerfile: Dockerfile
    container_name: iot-storage
    restart: always
    depends_on:
      postgres:
        condition: service_healthy
      mqtt:
        condition: service_started
    environment:
      Broker__Mode: mqtt
      Broker__Mqtt__Host: iot-mqtt
      Broker__Mqtt__Port: "1883"
      Broker__Mqtt__Topic: iot/sensors
      Broker__Mqtt__QoS: "1"
      Database__ConnectionString: "Host=iot-postgres;Port=5432;Database=iot_db;Username=postgres;Password=postgres"
      Batching__BatchSize: "500"
      Batching__FlushIntervalSeconds: "5"
```

Za Kafka mod dodaj `Broker__Kafka__*` varijable iz sekcije iznad (override
preko `docker-compose.override.yml` za testiranje, ili zameni `Broker__Mode`
i dodaj ih trajno kad se odluči koji mod je "default").

## Status

- ✅ MQTT mod (`MqttConsumer`) — implementiran, integrisan u docker-compose
- ✅ Kafka mod (`KafkaConsumer`) — implementiran (Confluent.Kafka), čeka
  end-to-end test sa pravim Kafka brokerom (vidi napomenu o topic imenu iznad)
- ⏳ acks testiranje (0/1/all) — radi se na strani Kafka producer benchmark
  skripte (`kafka-producer-perf-test.sh`), ne dotiče Data Storage kod
