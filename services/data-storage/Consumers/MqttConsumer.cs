using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MQTTnet;
using MQTTnet.Client;

namespace DataStorage.Consumers;

public class MqttConsumer : IMessageConsumer
{
    private readonly ILogger<MqttConsumer> _logger;
    private readonly IConfiguration _config;
    private IMqttClient? _client;
    private MqttClientOptions? _options;
    private CancellationToken _cancellationToken;

    public MqttConsumer(ILogger<MqttConsumer> logger, IConfiguration config)
    {
        _logger = logger;
        _config = config;
    }

    public async Task StartAsync(Func<string, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        _cancellationToken = cancellationToken;

        var host = _config["Broker:Mqtt:Host"] ?? "mqtt";
        var port = _config.GetValue<int>("Broker:Mqtt:Port", 1883);
        var clientId = _config["Broker:Mqtt:ClientId"] ?? "data-storage-service";
        var topic = _config["Broker:Mqtt:Topic"] ?? "iot/sensors";
        var qosLevel = _config.GetValue<int>("Broker:Mqtt:QoS", 1);
        var qos = (MQTTnet.Protocol.MqttQualityOfServiceLevel)qosLevel;

        var factory = new MqttFactory();
        _client = factory.CreateMqttClient();

        _options = new MqttClientOptionsBuilder()
            .WithTcpServer(host, port)
            .WithClientId(clientId)
            .WithCleanSession(false)
            .WithKeepAlivePeriod(TimeSpan.FromSeconds(30))
            .Build();

        _client.ApplicationMessageReceivedAsync += async e =>
        {
            try
            {
                var payload = e.ApplicationMessage.ConvertPayloadToString();
                await onMessageReceived(payload);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error handling MQTT message");
            }
        };

        _client.ConnectedAsync += async e =>
        {
            _logger.LogInformation("Connected to MQTT broker {Host}:{Port}", host, port);

            await _client.SubscribeAsync(new MqttTopicFilterBuilder()
                .WithTopic(topic)
                .WithQualityOfServiceLevel(qos)
                .Build());

            _logger.LogInformation("Subscribed to topic '{Topic}' with QoS {QoS}", topic, qosLevel);
        };

        _client.DisconnectedAsync += async e =>
        {
            if (_cancellationToken.IsCancellationRequested)
                return;

            _logger.LogWarning("Disconnected from MQTT broker ({Reason}). Reconnecting in 5s...", e.Reason);

            await Task.Delay(TimeSpan.FromSeconds(5), _cancellationToken);

            try
            {
                if (!_cancellationToken.IsCancellationRequested)
                    await _client.ConnectAsync(_options, _cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Reconnect attempt failed");
            }
        };

        await _client.ConnectAsync(_options, cancellationToken);
    }

    public async Task StopAsync()
    {
        if (_client is { IsConnected: true })
        {
            await _client.DisconnectAsync();
        }
    }
}
