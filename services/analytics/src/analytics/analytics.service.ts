import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import * as mqtt from 'mqtt';

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
  private client!: mqtt.MqttClient;

  private readonly MQTT_BROKER = 'mqtt://localhost:1883';
  private readonly TOPIC = 'iot/sensors';
  private readonly WINDOW_MS = 5000;       
  private readonly TEMP_THRESHOLD = 50;     // alert ako prosek iznad 50 stepeni

  private buffer: SensorPayload[] = [];     
  private windowTimer!: NodeJS.Timeout;

  onModuleInit() {
    this.client = mqtt.connect(this.MQTT_BROKER);

    this.client.on('connect', () => {
      this.logger.log(`Povezan na MQTT broker: ${this.MQTT_BROKER}`);
      this.client.subscribe(this.TOPIC);
      this.startTumblingWindow();
    });

    this.client.on('message', (topic, message) => {
      try {
        const payload: SensorPayload = JSON.parse(message.toString());
        this.buffer.push(payload);
      } catch (err) {
        this.logger.error('Greska pri parsiranju poruke');
      }
    });

    this.client.on('error', (err) => {
      this.logger.error(`MQTT greska: ${err.message}`);
    });
  }

  private startTumblingWindow() {
    this.windowTimer = setInterval(() => {
      this.processWindow();
    }, this.WINDOW_MS);
  }

  private processWindow() {
    if (this.buffer.length === 0) {
      this.logger.log('Prozor: nema poruka');
      return;
    }

    const count = this.buffer.length;
    const avgTemp = this.buffer.reduce((sum, p) => sum + p.temperature, 0) / count;
    const avgHumidity = this.buffer.reduce((sum, p) => sum + p.humidity, 0) / count;
    const avgCo = this.buffer.reduce((sum, p) => sum + p.co, 0) / count;

    this.logger.log(`Prozor [10s] | Poruka: ${count} | ` + `Avg Temp: ${avgTemp.toFixed(2)}°C | ` + `Avg Humidity: ${avgHumidity.toFixed(2)}% | ` + `Avg CO: ${avgCo.toFixed(4)}`);

    if (avgTemp > this.TEMP_THRESHOLD) {
      this.logger.error(`UPOZORENEJ!!!! Prosecna temperatura ${avgTemp.toFixed(2)}°C ` + `prelazi prag od ${this.TEMP_THRESHOLD}°C!`);
    }

    this.buffer = [];
  }

  onModuleDestroy() {
    clearInterval(this.windowTimer);
    this.client.end();
    this.logger.log('Analytics servis zaustavljen.');
  }
}