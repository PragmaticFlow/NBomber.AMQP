using RabbitMQ.Client;
using NBomber.AMQP;
using NBomber.Contracts;
using NBomber.CSharp;

namespace IndependentActors;

public class ConsumeScenario
{
    private ConnectionFactory factory = new ConnectionFactory { HostName = "localhost" };

    public ScenarioProps Create()
    {
        return Scenario.Create("consume_scenario", async ctx =>
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

            var subscribe = await Step.Run("subscribe", ctx, async () =>
            {
                var amqpClient = (AmqpClient)ctx.Data["amqpClient"];
                var queueName = ctx.ScenarioInfo.InstanceId;
                return await amqpClient.Subscribe(queue: queueName, autoAck: true);
            });

            var receive = await Step.Run("receive", ctx, async () =>
            {
                var amqpClient = (AmqpClient)ctx.Data["amqpClient"];
                var response = await amqpClient.Receive().AsTask();

                return response;
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
