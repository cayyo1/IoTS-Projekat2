import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import * as mqtt from 'mqtt';
import { Kafka, Producer } from 'kafkajs';

interface SensorPayload {
  deviceId: string;
  timestamp: number;
  co: number;
  humidity: number;
  light: boolean;
  smoke: number;
  temperature: number;
}

@Injectable()
export class IngestionService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(IngestionService.name);

  private readonly BROKER_TYPE: string = process.env.BROKER_TYPE || 'mqtt';

  private readonly DEVICE_COUNT = 3;
  private readonly INTERVAL_MS = 2000;

  private readonly MQTT_BROKER = process.env.MQTT_BROKER || 'mqtt://localhost:1883';
  private readonly KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9093';
  private readonly MQTT_TOPIC = 'iot/sensors';
  private readonly KAFKA_TOPIC = 'iot.sensors'; 
  private readonly KAFKA_ACKS = parseInt(process.env.KAFKA_ACKS || '1') as 0 | 1 | -1; 

  private readonly RANGES = {
    co:          { min: 0.00, max: 0.02  },
    humidity:    { min: 0.0,  max: 100.0 },
    smoke:       { min: 0.00, max: 0.06  },
    temperature: { min: 0.0,  max: 100.0 },
  };

  // MQTT
  private mqttClient!: mqtt.MqttClient;

  // Kafka
  private kafka!: Kafka;
  private producer!: Producer;

  private intervals: NodeJS.Timeout[] = [];

  async onModuleInit() {
    if (this.BROKER_TYPE === 'mqtt') {
      await this.initMqtt();
    } else {
      await this.initKafka();
    }
  }

  // ─── MQTT ───────────────────────────────────────────
  private initMqtt() {
    this.mqttClient = mqtt.connect(this.MQTT_BROKER);

    this.mqttClient.on('connect', () => {
      this.logger.log(`[MQTT] Povezan na broker: ${this.MQTT_BROKER}`);
      this.startDevices();
    });

    this.mqttClient.on('error', (err) => {
      this.logger.error(`[MQTT] Greška: ${err.message}`);
    });
  }

  private publishMqtt(payload: SensorPayload) {
    this.mqttClient.publish(this.MQTT_TOPIC, JSON.stringify(payload));
  }

  // ─── KAFKA ──────────────────────────────────────────

  private async initKafka() {
    this.kafka = new Kafka({
      clientId: 'data-ingestion',
      brokers: [this.KAFKA_BROKER],
    });

    this.producer = this.kafka.producer({
      allowAutoTopicCreation: true,
    });
    
    await this.producer.connect();
    this.logger.log(`[Kafka] Povezan na broker: ${this.KAFKA_BROKER} | acks: ${this.KAFKA_ACKS}`);
    this.startDevices();
  }

  private async publishKafka(payload: SensorPayload) {
    await this.producer.send({
      topic: this.KAFKA_TOPIC,
      acks: this.KAFKA_ACKS,
      messages: [{ value: JSON.stringify(payload) }],
    });
  }

  // ─── ZAJEDNIČKA LOGIKA ───────────────────────────────
  private startDevices() {
    this.logger.log(`Pokretanje ${this.DEVICE_COUNT} uređaja [${this.BROKER_TYPE.toUpperCase()}]...`);

    for (let i = 1; i <= this.DEVICE_COUNT; i++) {
      const deviceId = `sensor-${String(i).padStart(3, '0')}`;

      const interval = setInterval(async () => {
        const payload = this.generatePayload(deviceId);

        if (this.BROKER_TYPE === 'mqtt') {
          this.publishMqtt(payload);
        } else {
          await this.publishKafka(payload);
        }
      }, this.INTERVAL_MS);

      this.intervals.push(interval);
    }

    const topic = this.BROKER_TYPE === 'mqtt' ? this.MQTT_TOPIC : this.KAFKA_TOPIC;
    this.logger.log(`Svi uređaji aktivni – topic: ${topic}`);
  }

  private generatePayload(deviceId: string): SensorPayload {
    return {
      deviceId,
      timestamp: Date.now(),
      co:          this.random(this.RANGES.co.min,          this.RANGES.co.max),
      humidity:    this.random(this.RANGES.humidity.min,    this.RANGES.humidity.max),
      light:       Math.random() > 0.5,
      smoke:       this.random(this.RANGES.smoke.min,       this.RANGES.smoke.max),
      temperature: this.random(this.RANGES.temperature.min, this.RANGES.temperature.max),
    };
  }

  private random(min: number, max: number): number {
    return parseFloat((Math.random() * (max - min) + min).toFixed(4));
  }

  async onModuleDestroy() {
    this.intervals.forEach(clearInterval);

    if (this.BROKER_TYPE === 'mqtt') {
      this.mqttClient.end();
    } else {
      await this.producer.disconnect();
    }

    this.logger.log('Ingestion servis zaustavljen.');
  }
}