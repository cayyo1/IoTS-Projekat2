using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using DataStorage.Models;

namespace DataStorage.Data;

public class SensorDataRepository
{
    private readonly string _connectionString;
    private readonly ILogger<SensorDataRepository> _logger;

    public SensorDataRepository(IConfiguration config, ILogger<SensorDataRepository> logger)
    {
        _connectionString = config["Database:ConnectionString"]
            ?? throw new InvalidOperationException("Database:ConnectionString is not configured");
        _logger = logger;
    }

    public async Task BulkInsertAsync(IReadOnlyList<SensorReading> readings, CancellationToken cancellationToken)
    {
        if (readings.Count == 0)
            return;

        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var writer = await connection.BeginBinaryImportAsync(
            "COPY sensor_data (device_id, timestamp, co, humidity, light, smoke, temperature) " +
            "FROM STDIN (FORMAT BINARY)",
            cancellationToken);

        foreach (var reading in readings)
        {
            await writer.StartRowAsync(cancellationToken);
            await writer.WriteAsync(reading.DeviceId, NpgsqlDbType.Varchar, cancellationToken);
            await writer.WriteAsync(reading.Timestamp, NpgsqlDbType.Double, cancellationToken);
            await writer.WriteAsync(reading.Co, NpgsqlDbType.Double, cancellationToken);
            await writer.WriteAsync(reading.Humidity, NpgsqlDbType.Double, cancellationToken);
            await writer.WriteAsync(reading.Light, NpgsqlDbType.Boolean, cancellationToken);
            await writer.WriteAsync(reading.Smoke, NpgsqlDbType.Double, cancellationToken);
            await writer.WriteAsync(reading.Temperature, NpgsqlDbType.Double, cancellationToken);
        }

        await writer.CompleteAsync(cancellationToken);

        _logger.LogDebug("Inserted {Count} rows into sensor_data", readings.Count);
    }
}
