namespace DataStorage.Models;

public class SensorReading
{
    public string DeviceId { get; set; } = string.Empty;

    public double Timestamp { get; set; }

    public double Co { get; set; }
    public double Humidity { get; set; }
    public bool Light { get; set; }
    public double Smoke { get; set; }
    public double Temperature { get; set; }
}
