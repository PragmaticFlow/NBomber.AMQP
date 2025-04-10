using RabbitMQ.Client;
using NBomber.AMQP;
using NBomber.Contracts;
using NBomber.CSharp;

namespace IndependentActors;

public class ConsumeScenario
{
    private ConnectionFactory factory = new ConnectionFactory { HostName = "localhost" };
    private IConnection connection = null;
    private IChannel channel = null;
    private AmqpClient amqpClient = null;

    public ScenarioProps Create()
    {
        return Scenario.Create("consume_scenario", async ctx =>
        {
            var subscribe = await Step.Run("subscribe", ctx, async () =>
            {
                return await amqpClient.Subscribe(queue: "IndependentActors", autoAck: true);
            });

            var receive = await Step.Run("receive", ctx, async () =>
            {
                return await amqpClient.Receive().AsTask();
            });

            return Response.Ok();
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
        })
        .WithClean(async ctx =>
        {
            await connection.DisposeAsync();
            await amqpClient.Disconnect();
            await channel.DisposeAsync();
        });
    }
}
