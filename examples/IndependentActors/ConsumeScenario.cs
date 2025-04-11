using RabbitMQ.Client;
using NBomber.AMQP;
using NBomber.Contracts;
using NBomber.CSharp;

namespace IndependentActors;

public class ConsumeScenario
{  
    public ScenarioProps Create()
    {
        ConnectionFactory factory = new ConnectionFactory { HostName = "localhost" };
        IConnection connection = null;
        IChannel channel = null;
        AmqpClient amqpClient = null;

        return Scenario.Create("consume_scenario", async ctx =>
        {
            var message = await amqpClient.Receive();

            // Final latency is computed by subtracting the current time from the timestamp in the header.
            var timestampMs = (long)message.Payload.Value.BasicProperties.Headers["timestamp"];
            var latency = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - timestampMs;

            return Response.Ok(customLatencyMs: latency);
        })        
        .WithoutWarmUp()
        .WithLoadSimulations(
            Simulation.KeepConstant(1, TimeSpan.FromSeconds(30))
        )
        .WithInit(async ctx =>
        {
            connection = await factory.CreateConnectionAsync();
            channel = await connection.CreateChannelAsync();
            amqpClient = new AmqpClient(channel);

            await amqpClient.Connect(exchange: "myExchange", exchangeType: ExchangeType.Direct, queue: "IndependentActors",
                routingKey: "IndependentActors");

            await amqpClient.Subscribe(queue: "IndependentActors", autoAck: true);
        })
        .WithClean(async ctx =>
        {
            await connection.DisposeAsync();
            await amqpClient.Disconnect();
            await channel.DisposeAsync();
        });
    }
}
