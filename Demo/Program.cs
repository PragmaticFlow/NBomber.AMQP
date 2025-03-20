using NBomber.AMQP;
using NBomber.CSharp;
using NBomber.Data;
using RabbitMQ.Client;

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
                
                await amqpClient.Channel.ExchangeDeclareAsync(exchange: "myExchange", type: ExchangeType.Topic);//Direct

                var topicName = ctx.ScenarioInfo.InstanceId;
                await amqpClient.Channel.BasicPublishAsync(exchange: "myExchange", routingKey: topicName, body: payload);

                var prop = new BasicProperties();

                await amqpClient.Channel.QueueBindAsync(queue: topicName, exchange: "myExchange", 
                    routingKey: topicName);
                
                amqpClient.AddConsumer(queue: topicName, autoAck: true);
                
                var publish = Step.Run("publish", ctx, async () =>
                {
                    return amqpClient.Publish(exchange: "myExchange", routingKey: topicName, prop, body: payload);
                });

                var receive = Step.Run("receive", ctx, () =>
                {
                    return amqpClient.Receive().AsTask();
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