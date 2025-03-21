using NBomber.AMQP;
using NBomber.CSharp;
using NBomber.Data;
using RabbitMQ.Client;
using System.Runtime.CompilerServices;

await new PingPongAmqpTest().Run();

public class PingPongAmqpTest
{
    public async Task Run()
    {
        var payload = Data.GenerateRandomBytes(200);
        
        var factory = new ConnectionFactory { HostName = "localhost" };
        using var connection = await factory.CreateConnectionAsync();
        using var channel = await connection.CreateChannelAsync();

        var scenario = Scenario.Create("ping_pong_amqp_scenario", async ctx =>
            {
                var amqpClient = new AmqpClient(channel);
                var prop = new BasicProperties();
                var scenarioInstanceId = ctx.ScenarioInfo.InstanceId;

                var connect = await Step.Run("connect", ctx, async () =>
                {
                    return amqpClient.Connect(exchange: "myExchange", exchangeType: ExchangeType.Direct, queue: scenarioInstanceId,
                        routingKey: scenarioInstanceId);
                });

                var subscribe = await Step.Run("subscribe", ctx, async () =>
                {
                    return amqpClient.Subscribe(queue: scenarioInstanceId, autoAck: true);
                });                
                
                var publish = await Step.Run("publish", ctx, async () =>
                {
                    return amqpClient.Publish(exchange: "myExchange", routingKey: scenarioInstanceId, prop, body: payload);
                });

                var receive = await Step.Run("receive", ctx, () =>
                {
                    return amqpClient.Receive().AsTask();
                });

                var disconnect = await Step.Run("disconnect", ctx, async () =>
                {
                    return amqpClient.Disconnect();
                });

                return Response.Ok();
            })
        .WithoutWarmUp()
        .WithLoadSimulations(
            Simulation.KeepConstant(1, TimeSpan.FromSeconds(30))
        );
        
        NBomberRunner
            .RegisterScenarios(scenario)
            .Run();

        await connection.CloseAsync();
    }
}