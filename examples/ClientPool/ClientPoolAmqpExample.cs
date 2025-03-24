using Microsoft.Extensions.Configuration;
using NBomber;
using NBomber.AMQP;
using NBomber.CSharp;
using NBomber.Data;
using RabbitMQ.Client;

new ClientPoolAmqpExample().Run();

public class CustomScenarioSettings
{
    public string AmqpServerUrl { get; set; }
    public int ClientCount { get; set; }
    public int MsgSizeBytes { get; set; }
}

public class ClientPoolAmqpExample
{
    public void Run()
    {
        var clientPool = new ClientPool<AmqpClient>();
        var message = Array.Empty<byte>();

        var scenario = Scenario.Create("amqp_scenario", async ctx =>
        {
            var client = clientPool.GetClient(ctx.ScenarioInfo);
            var scenarioInstanceId = ctx.ScenarioInfo.InstanceId;
            var prop = new BasicProperties();

            var publish = await Step.Run("publish", ctx, async () =>
            {
                var response = client.Publish(exchange: "myExchange", routingKey: scenarioInstanceId, basicProperties: prop, body: message);
                return response;
            });

            var receive = await Step.Run("receive", ctx, async () =>
            {
                var response = await client.Receive().AsTask();
                return response;
            });

            return Response.Ok();
        })
        .WithWarmUpDuration(TimeSpan.FromSeconds(3))
        .WithLoadSimulations(Simulation.KeepConstant(copies: 1, during: TimeSpan.FromSeconds(30)))
        .WithInit(async context =>
        {
            var config = context.CustomSettings.Get<CustomScenarioSettings>();
            message = Data.GenerateRandomBytes(config.MsgSizeBytes);

            var factory = new ConnectionFactory { HostName = config.AmqpServerUrl };
            var connection = await factory.CreateConnectionAsync();
            var channel = await connection.CreateChannelAsync();

            for (var i = 0; i < config.ClientCount; i++)
            {
                var amqpClient = new AmqpClient(channel);
                var scenarioInstanceId = $"amqp_scenario_{i}";
                var result = amqpClient.Connect(exchange: "myExchange", exchangeType: ExchangeType.Direct, queue: scenarioInstanceId,
                        routingKey: scenarioInstanceId);

                if (!result.IsError)
                {
                    amqpClient.Subscribe(queue: scenarioInstanceId);
                    clientPool.AddClient(amqpClient);
                }
                else
                    throw new Exception("client can't connect to the AMQP broker");
            }
        })
        .WithClean(ctx =>
        {
            clientPool.DisposeClients(client => client.Disconnect());
            return Task.CompletedTask;
        });

        NBomberRunner
            .RegisterScenarios(scenario)
            .LoadConfig("./config.json")
            .Run();
    }
}