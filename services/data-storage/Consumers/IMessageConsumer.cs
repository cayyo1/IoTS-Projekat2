namespace DataStorage.Consumers;

/// <summary>
/// Abstraction over the message broker used to receive sensor data.
/// Implemented by <see cref="MqttConsumer"/> for MQTT, and (later)
/// by a KafkaConsumer once the Kafka broker is added to docker-compose.
///
/// Selecting the implementation is done in Program.cs based on the
/// "Broker:Mode" configuration value ("mqtt" or "kafka").
/// </summary>
public interface IMessageConsumer
{
    /// <summary>
    /// Connects to the broker and starts receiving messages.
    /// <paramref name="onMessageReceived"/> is invoked (as raw JSON string) for every message.
    /// </summary>
    Task StartAsync(Func<string, Task> onMessageReceived, CancellationToken cancellationToken);

    /// <summary>Gracefully disconnects from the broker.</summary>
    Task StopAsync();
}
