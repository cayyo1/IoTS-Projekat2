namespace DataStorage.Models;

/// <summary>
/// Represents a single sensor reading published by the Data Ingestion service
/// on the "iot/sensors" topic (and, later, the equivalent Kafka topic).
///
/// JSON payload example:
/// {
///   "deviceId": "sensor-001",
///   "timestamp": 1750000000000,
///   "co": 0.0123,
///   "humidity": 45.2,
///   "light": true,
///   "smoke": 0.0231,
///   "temperature": 22.5
/// }
/// </summary>
public class SensorReading
{
    public string DeviceId { get; set; } = string.Empty;

    /// <summary>Unix timestamp in milliseconds, as sent by the ingestion service (Date.now()).</summary>
    public double Timestamp { get; set; }

    public double Co { get; set; }
    public double Humidity { get; set; }
    public bool Light { get; set; }
    public double Smoke { get; set; }
    public double Temperature { get; set; }
}
