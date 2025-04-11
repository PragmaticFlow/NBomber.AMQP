using RabbitMQ.Client;
using NBomber.AMQP;
using NBomber.Contracts;
using NBomber.CSharp;
using NBomber.Data;

namespace IndependentActors;

public class PublishScenario
{
    public ScenarioProps Create()
    {
        byte[] payload = Data.GenerateRandomBytes(200);
        ConnectionFactory factory = new ConnectionFactory { HostName = "localhost" };
        IConnection connection = null;
        IChannel channel = null;
        AmqpClient amqpClient = null;

        return Scenario.Create("publish_scenario", async ctx =>
        {
            var publish = await Step.Run("publish", ctx, async () =>
            {
                var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                var prop = new BasicProperties
                {
                    // We attach current timestamp to calculate final latency on consumer side
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
