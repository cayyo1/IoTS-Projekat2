namespace DataStorage.Consumers;

public interface IMessageConsumer
{

    Task StartAsync(Func<string, Task> onMessageReceived, CancellationToken cancellationToken);

    Task StopAsync();
}
