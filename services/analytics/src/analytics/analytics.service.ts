import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import * as mqtt from 'mqtt';
import { Kafka, Consumer, EachMessagePayload } from 'kafkajs';

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
export class AnalyticsService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(AnalyticsService.name);

  // ⚙️ Menjaj ovde
  private readonly BROKER_TYPE: string = 'kafka'; // 'mqtt' ili 'kafka'

  private readonly MQTT_BROKER = process.env.MQTT_BROKER || 'mqtt://localhost:1883';
  private readonly KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9093';
  private readonly TOPIC = 'iot/sensors';
  private readonly WINDOW_MS = 10000;
  private readonly TEMP_THRESHOLD = 50;

  // MQTT
  private mqttClient!: mqtt.MqttClient;

  // Kafka
  private kafka!: Kafka;
  private consumer!: Consumer;

  private buffer: SensorPayload[] = [];
  private windowTimer!: NodeJS.Timeout;

  async onModuleInit() {
    this.startTumblingWindow();

    if (this.BROKER_TYPE === 'mqtt') {
      this.initMqtt();
    } else {
      await this.initKafka();
    }
  }

  // ─── MQTT ───────────────────────────────────────────
  private initMqtt() {
    this.mqttClient = mqtt.connect(this.MQTT_BROKER);

    this.mqttClient.on('connect', () => {
      this.logger.log(`[MQTT] Povezan na broker: ${this.MQTT_BROKER}`);
      this.mqttClient.subscribe(this.TOPIC);
    });

    this.mqttClient.on('message', (topic, message) => {
      this.handleMessage(message.toString());
    });

    this.mqttClient.on('error', (err) => {
      this.logger.error(`[MQTT] Greška: ${err.message}`);
    });
  }

  // ─── KAFKA ──────────────────────────────────────────
  private async initKafka() {
    this.kafka = new Kafka({
      clientId: 'analytics',
      brokers: [this.KAFKA_BROKER],
    });

    this.consumer = this.kafka.consumer({ groupId: 'analytics-group' });
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: this.TOPIC, fromBeginning: false });

    this.logger.log(`[Kafka] Povezan na broker: ${this.KAFKA_BROKER}`);

    await this.consumer.run({
      eachMessage: async ({ message }: EachMessagePayload) => {
        if (message.value) {
          this.handleMessage(message.value.toString());
        }
      },
    });
  }

  // ─── ZAJEDNIČKA LOGIKA ───────────────────────────────
  private handleMessage(raw: string) {
    try {
      const payload: SensorPayload = JSON.parse(raw);
      this.buffer.push(payload);
    } catch {
      this.logger.error('Greška pri parsiranju poruke');
    }
  }

  private startTumblingWindow() {
    this.windowTimer = setInterval(() => {
      this.processWindow();
    }, this.WINDOW_MS);
  }

  private processWindow() {
    if (this.buffer.length === 0) {
      this.logger.log('Prozor [10s]: nema poruka');
      return;
    }

    const count = this.buffer.length;
    const avgTemp     = this.buffer.reduce((s, p) => s + p.temperature, 0) / count;
    const avgHumidity = this.buffer.reduce((s, p) => s + p.humidity,    0) / count;
    const avgCo       = this.buffer.reduce((s, p) => s + p.co,          0) / count;

    this.logger.log(
      `Prozor [10s] | Poruka: ${count} | ` +
      `Avg Temp: ${avgTemp.toFixed(2)}°C | ` +
      `Avg Humidity: ${avgHumidity.toFixed(2)}% | ` +
      `Avg CO: ${avgCo.toFixed(4)}`
    );

    if (avgTemp > this.TEMP_THRESHOLD) {
      this.logger.error(
        `🚨 CRITICAL ALERT – Prosečna temperatura ${avgTemp.toFixed(2)}°C ` +
        `prelazi prag od ${this.TEMP_THRESHOLD}°C!`
      );
    }

    this.buffer = [];
  }

  async onModuleDestroy() {
    clearInterval(this.windowTimer);

    if (this.BROKER_TYPE === 'mqtt') {
      this.mqttClient.end();
    } else {
      await this.consumer.disconnect();
    }

    this.logger.log('Analytics servis zaustavljen.');
  }
}