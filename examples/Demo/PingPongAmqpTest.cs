using NBomber.AMQP;
using NBomber.CSharp;
using NBomber.Data;
using RabbitMQ.Client;

new PingPongAmqpTest().Run();

public class PingPongAmqpTest
{
    public void Run()
    {
        var payload = Data.GenerateRandomBytes(200);
        var factory = new ConnectionFactory { HostName = "localhost" };

        var scenario = Scenario.Create("ping_pong_amqp_scenario", async ctx =>
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
                
                var publish = await Step.Run("publish", ctx, async () =>
                {
                    var amqpClient = (AmqpClient)ctx.Data["amqpClient"];
                    var queueName = ctx.ScenarioInfo.InstanceId;
                    var prop = new BasicProperties();
                    return await amqpClient.Publish(exchange: "myExchange", routingKey: queueName, prop, body: payload);
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
        
        NBomberRunner
            .RegisterScenarios(scenario)
            .Run();
    }
}