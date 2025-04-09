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

    public ScenarioProps Create(string id)
    {
        return Scenario.Create("publish_scenario_" + id, async ctx =>
        {
            var connect = await Step.Run("connect", ctx, async () =>
            {
                var connection = await factory.CreateConnectionAsync();
                ctx.Data["connection"] = connection;

                var channel = await connection.CreateChannelAsync();
                ctx.Data["channel"] = channel;

                var amqpClient = new AmqpClient(channel);
                ctx.Data["amqpClient"] = amqpClient;

                var scenarioInstanceId = ctx.ScenarioInfo.InstanceId;

                return await amqpClient.Connect(exchange: "myExchange", exchangeType: ExchangeType.Direct, queue: scenarioInstanceId,
                    routingKey: scenarioInstanceId);
            });

            var publish = await Step.Run("publish", ctx, async () =>
            {
                var amqpClient = (AmqpClient)ctx.Data["amqpClient"];
                var queueName = ctx.ScenarioInfo.InstanceId;

                var timestamp = DateTime.UtcNow.Millisecond;
                var prop = new BasicProperties
                {
                    Headers = new Dictionary<string, object?>
                    {
                        { "timestamp", timestamp }
                    }
                };

                return await amqpClient.Publish(exchange: "myExchange", routingKey: queueName, prop, body: payload);
            });

            var disconnect = await Step.Run("disconnect", ctx, async () =>
            {
                var connection = (IConnection)ctx.Data["connection"];
                await connection.DisposeAsync();

                var amqpClient = (AmqpClient)ctx.Data["amqpClient"];
                await amqpClient.Disconnect();

                var channel = (IChannel)ctx.Data["channel"];
                await channel.DisposeAsync();

                return Response.Ok();
            });

            return Response.Ok();
        })
        .WithoutWarmUp()
        .WithLoadSimulations(
            Simulation.KeepConstant(1, TimeSpan.FromSeconds(30))
        );
    }
}
