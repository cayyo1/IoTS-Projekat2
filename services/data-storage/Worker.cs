using System.Text.Json;
using System.Threading.Channels;
using DataStorage.Consumers;
using DataStorage.Data;
using DataStorage.Models;

namespace DataStorage;


public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IMessageConsumer _consumer;
    private readonly SensorDataRepository _repository;
    private readonly Channel<SensorReading> _channel;
    private readonly int _batchSize;
    private readonly TimeSpan _flushInterval;
    private readonly bool _disableWrites;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public Worker(
        ILogger<Worker> logger,
        IMessageConsumer consumer,
        SensorDataRepository repository,
        IConfiguration config)
    {
        _logger = logger;
        _consumer = consumer;
        _repository = repository;
        _channel = Channel.CreateUnbounded<SensorReading>();

        _batchSize = config.GetValue("Batching:BatchSize", 500);
        _flushInterval = TimeSpan.FromSeconds(config.GetValue("Batching:FlushIntervalSeconds", 5));
        _disableWrites = config.GetValue("Batching:DisableWrites", false);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "Starting Data Storage Service. BatchSize={BatchSize}, FlushInterval={FlushInterval}s, DisableWrites={DisableWrites}",
            _batchSize, _flushInterval.TotalSeconds, _disableWrites);

        await _consumer.StartAsync(OnMessageReceivedAsync, stoppingToken);

        var buffer = new List<SensorReading>(_batchSize);
        var lastFlush = DateTime.UtcNow;

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var hasDataTask = _channel.Reader.WaitToReadAsync(stoppingToken).AsTask();
                var timeoutTask = Task.Delay(_flushInterval, stoppingToken);

                await Task.WhenAny(hasDataTask, timeoutTask);

                while (_channel.Reader.TryRead(out var reading))
                {
                    buffer.Add(reading);

                    if (buffer.Count >= _batchSize)
                    {
                        await FlushAsync(buffer, stoppingToken);
                        lastFlush = DateTime.UtcNow;
                    }
                }

                if (buffer.Count > 0 && (DateTime.UtcNow - lastFlush) >= _flushInterval)
                {
                    await FlushAsync(buffer, stoppingToken);
                    lastFlush = DateTime.UtcNow;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // normal shutdown
        }
        finally
        {
            if (buffer.Count > 0)
                await FlushAsync(buffer, CancellationToken.None);

            await _consumer.StopAsync();
        }
    }

    private Task OnMessageReceivedAsync(string payload)
    {
        try
        {
            var reading = JsonSerializer.Deserialize<SensorReading>(payload, JsonOptions);
            if (reading is not null)
            {
                // Channel is unbounded -> TryWrite always succeeds.
                _channel.Writer.TryWrite(reading);
            }
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Failed to deserialize message: {Payload}", payload);
        }

        return Task.CompletedTask;
    }

    private async Task FlushAsync(List<SensorReading> buffer, CancellationToken cancellationToken)
    {
        var count = buffer.Count;

        if (_disableWrites)
        {
            _logger.LogInformation("DisableWrites=true -> discarding {Count} readings", count);
            buffer.Clear();
            return;
        }
        
        try
        {
            await _repository.BulkInsertAsync(buffer, cancellationToken);
            _logger.LogInformation("Flushed {Count} readings to sensor_data", count);
            buffer.Clear();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to flush {Count} readings to database - retry sledeci put", count);
            // bafer se NE brise - pokusavamo ponovo na sledecem flush-u
        }
    }
}
