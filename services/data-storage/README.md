# Data Storage Service (.NET)

ASP.NET Core Worker Service koji se pretplaćuje na broker (trenutno MQTT,
Kafka mod planiran kasnije) i upisuje primljene senzorske podatke u
PostgreSQL (`sensor_data` tabela iz `db/init.sql`).

## Struktura

```
data-storage/
├── DataStorage.csproj
├── Program.cs                  -> DI setup, izbor MQTT/Kafka konzumera
├── Worker.cs                   -> batching logika (500 poruka ili 5s)
├── appsettings.json            -> default (docker) konfiguracija
├── appsettings.Development.json-> konfiguracija za lokalno pokretanje (localhost)
├── Dockerfile
├── Models/SensorReading.cs     -> mapira JSON payload sa ingestion servisa
├── Consumers/
│   ├── IMessageConsumer.cs     -> interfejs (MQTT/Kafka swap)
│   └── MqttConsumer.cs         -> MQTTnet implementacija
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
| `Broker__Mode`                    | `mqtt`               | `mqtt` ili `kafka` (kasnije)   |
| `Broker__Mqtt__Host`              | `mqtt`               | hostname MQTT brokera          |
| `Broker__Mqtt__Port`              | `1883`               | port                            |
| `Broker__Mqtt__Topic`             | `iot/sensors`        | topic                           |
| `Broker__Mqtt__QoS`               | `1`                  | 0, 1 ili 2 – za QoS testiranje  |
| `Database__ConnectionString`      | (postgres@docker)    | Npgsql connection string       |
| `Batching__BatchSize`             | `500`                | prag za bulk insert            |
| `Batching__FlushIntervalSeconds`  | `5`                  | max vreme do flush-a            |
| `Batching__DisableWrites`         | `false`              | true = ne upisuje u bazu         |

To znači da za **QoS testiranje (Obojica)** samo treba pokrenuti servis tri
puta sa `Broker__Mqtt__QoS=0`, `=1`, `=2` i meriti latenciju/gubitak poruka.

## Dodavanje u docker-compose.yml

```yaml
  data-storage:
    build:
      context: ./services/data-storage
      dockerfile: Dockerfile
    container_name: iot-data-storage
    restart: always
    environment:
      Broker__Mode: mqtt
      Broker__Mqtt__Host: mqtt
      Broker__Mqtt__Port: "1883"
      Broker__Mqtt__Topic: iot/sensors
      Broker__Mqtt__QoS: "1"
      Database__ConnectionString: "Host=postgres;Port=5432;Database=iot_db;Username=postgres;Password=postgres"
      Batching__BatchSize: "500"
      Batching__FlushIntervalSeconds: "5"
    depends_on:
      - postgres
      - mqtt
```

## TODO (kasnije)

- `Consumers/KafkaConsumer.cs` – implementacija `IMessageConsumer` za Kafku
  (Confluent.Kafka paket), birana kada je `Broker__Mode=kafka`. Treba dodati
  i `acks` konfiguraciju (0/1/all) na strani producer-a/consumer-a po potrebi
  za testiranje.
- Kada partner doda Kafka broker u docker-compose, samo treba dodati novi
  case u `Program.cs` switch i odgovarajuće env varijable.
