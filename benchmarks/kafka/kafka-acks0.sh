#!/bin/bash
echo "=== Kafka acks=0 ==="
docker exec iot-kafka /opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic iot.sensors \
  --num-records 50000 \
  --record-size 256 \
  --throughput -1 \
  --producer-props bootstrap.servers=localhost:9092 acks=0


