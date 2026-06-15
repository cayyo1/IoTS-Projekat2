using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace DataStorage.Consumers;

public class KafkaConsumer : IMessageConsumer
{
    private readonly ILogger<KafkaConsumer> _logger;
    private readonly IConfiguration _config;

    private IConsumer<Ignore, string>? _consumer;
    private CancellationTokenSource? _cts;
    private Task? _consumeTask;

    public KafkaConsumer(ILogger<KafkaConsumer> logger, IConfiguration config)
    {
        _logger = logger;
        _config = config;
    }

    public Task StartAsync(Func<string, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        var bootstrapServers = _config["Broker:Kafka:BootstrapServers"] ?? "iot-kafka:9092";
        var topic = _config["Broker:Kafka:Topic"] ?? "iot/sensors";
        var groupId = _config["Broker:Kafka:GroupId"] ?? "data-storage-group";
        var autoOffsetResetStr = _config["Broker:Kafka:AutoOffsetReset"] ?? "earliest";

        var autoOffsetReset = autoOffsetResetStr.Equals("latest", StringComparison.OrdinalIgnoreCase)
            ? AutoOffsetReset.Latest
            : AutoOffsetReset.Earliest;

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = autoOffsetReset,
            EnableAutoCommit = true,
        };

        _consumer = new ConsumerBuilder<Ignore, string>(consumerConfig)
            .SetErrorHandler((_, e) =>
                _logger.LogError("Kafka error: {Reason} (Code={Code}, Fatal={IsFatal})", e.Reason, e.Code, e.IsFatal))
            .Build();

        _consumer.Subscribe(topic);

        _logger.LogInformation(
            "Connected to Kafka {Brokers}, subscribed to topic '{Topic}' (group '{GroupId}', offset reset '{Reset}')",
            bootstrapServers, topic, groupId, autoOffsetReset);

        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        _consumeTask = Task.Run(async () =>
        {
            while (!_cts.Token.IsCancellationRequested)
            {
                try
                {
                    var result = _consumer.Consume(_cts.Token);

                    if (result?.Message?.Value != null)
                    {
                        await onMessageReceived(result.Message.Value);
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Kafka consume error: {Reason}", ex.Error.Reason);
                }
            }
        }, _cts.Token);

        return Task.CompletedTask;
    }

    public async Task StopAsync()
    {
        _cts?.Cancel();

        if (_consumeTask != null)
        {
            try
            {
                await _consumeTask;
            }
            catch (OperationCanceledException)
            {
                // expected on shutdown
            }
        }

        try
        {
            _consumer?.Close();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error while closing Kafka consumer");
        }
        finally
        {
            _consumer?.Dispose();
        }
    }
}
