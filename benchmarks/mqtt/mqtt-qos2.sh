#!/bin/bash
echo "=== MQTT QoS 2 - 100 klijenata ==="
docker run --rm --network iots-2_default emqx/emqtt-bench pub \
  -h iot-mqtt -t iot/sensors -c 100 -I 10 -s 256 -q 2 -n 500