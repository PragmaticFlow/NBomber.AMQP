using RabbitMQ.Client;
using NBomber.AMQP;
using NBomber.Contracts;
using NBomber.CSharp;
using NBomber.Data;

namespace IndependentActors;

public class PublishScenario
{
    private byte[] payload = Data.GenerateRandomBytes(200);
    private ConnectionFactory factory = new ConnectionFactory { HostName = "localhost" };
    private IConnection connection = null;
    private IChannel channel = null;
    private AmqpClient amqpClient = null;

    public ScenarioProps Create()
    {
        return Scenario.Create("publish_scenario", async ctx =>
        {
            var publish = await Step.Run("publish", ctx, async () =>
            {
                var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                var prop = new BasicProperties
                {
                    Headers = new Dictionary<string, object?>
                    {
                        { "timestamp", timestamp }
                    }
                };

                return await amqpClient.Publish(exchange: "myExchange", routingKey: "IndependentActors", prop, body: payload);
            });

            return Response.Ok();
        })
        .WithoutWarmUp()
        .WithLoadSimulations(
            Simulation.Inject(100, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30))
        )
        .WithInit(async ctx =>
        {
            connection = await factory.CreateConnectionAsync();
            channel = await connection.CreateChannelAsync();
            amqpClient = new AmqpClient(channel);

            await amqpClient.Connect(exchange: "myExchange", exchangeType: ExchangeType.Direct, queue: "IndependentActors",
                routingKey: "IndependentActors");
        })
        .WithClean(async ctx =>
        {
            await connection.DisposeAsync();
            await amqpClient.Disconnect();
            await channel.DisposeAsync();
        });
    }
}
