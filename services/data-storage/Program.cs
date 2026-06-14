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
        builder.Services.AddSingleton<IMessageConsumer, KafkaConsumer>();
        break;

    default:
        throw new InvalidOperationException($"Unknown Broker:Mode '{brokerMode}'. Use 'mqtt' or 'kafka'.");
}

builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
