import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import * as mqtt from 'mqtt';

@Injectable()
export class IngestionService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(IngestionService.name);
  private client!: mqtt.MqttClient;

  private readonly DEVICE_COUNT = 3;
  private readonly INTERVAL_MS = 2000;
  private readonly MQTT_BROKER = 'mqtt://localhost:1883';
  private readonly TOPIC = 'iot/sensors';

  private readonly RANGES = {
    co:          { min: 0.00,  max: 0.02  },
    humidity:    { min: 0.0,   max: 100.0 },
    smoke:       { min: 0.00,  max: 0.06  },
    temperature: { min: 0.0,   max: 100.0 },
  };

  private intervals: NodeJS.Timeout[] = [];

  onModuleInit() {
    this.client = mqtt.connect(this.MQTT_BROKER);

    this.client.on('connect', () => {
      this.logger.log(`Povezan na MQTT broker: ${this.MQTT_BROKER}`);
      this.startDevices();
    });

    this.client.on('error', (err) => {
      this.logger.error(`MQTT greska: ${err.message}`);
    });
  }

  private startDevices() {
    this.logger.log(`Pokretanje ${this.DEVICE_COUNT} uređaja...`);

    for (let i = 1; i <= this.DEVICE_COUNT; i++) {
      const deviceId = `sensor-${String(i).padStart(3, '0')}`;

      const interval = setInterval(() => {
        const payload = this.generatePayload(deviceId);
        this.client.publish(this.TOPIC, JSON.stringify(payload));
      }, this.INTERVAL_MS);

      this.intervals.push(interval);
    }

    this.logger.log(`Svi uređaji aktivni - salju na topic: ${this.TOPIC}`);
  }

  private generatePayload(deviceId: string) {
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

  onModuleDestroy() {
    this.intervals.forEach(clearInterval);
    this.client.end();
    this.logger.log('Ingestion servis zaustavljen.');
  }
}