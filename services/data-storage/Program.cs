using DataStorage;
using DataStorage.Consumers;
using DataStorage.Data;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSingleton<SensorDataRepository>();

var brokerMode = (builder.Configuration["Broker:Mode"] ?? "mqtt").ToLowerInvariant();

switch (brokerMode)
{
    case "mqtt":
        builder.Services.AddSingleton<IMessageConsumer, MqttConsumer>();
        break;

    case "kafka":
        // TODO: implement once the Kafka broker is added to docker-compose.
        // builder.Services.AddSingleton<IMessageConsumer, KafkaConsumer>();
        throw new NotSupportedException(
            "Kafka mode is not implemented yet. Set Broker:Mode=mqtt for now.");

    default:
        throw new InvalidOperationException($"Unknown Broker:Mode '{brokerMode}'. Use 'mqtt' or 'kafka'.");
}

builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
