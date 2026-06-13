======Pokretanje data-ingestion==========

1. U root folderu se pokrene komanda [docker compose up -d].

2. Pokrene se komanda [npm run start] u folderu [services/data-ingestion].

3. Otvara se jos jedan terminal i pokrece se komanda 
[docker exec -it iot-mqtt mosquitto_sub -t "iot/sensors" -v].


=======Pokretanje analytics===============

1. Pokrene se komanda [npm run start] u folderu [services/analytics].

2. Pokrene se komanda [npm run start] u folderu [services/data-ingestion].
