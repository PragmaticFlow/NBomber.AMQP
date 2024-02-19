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
        using var connection = factory.CreateConnection();
        using var channel = connection.CreateChannel();

        var scenario = Scenario.Create("ping_pong_amqp_scenario", async ctx =>
            {
                var amqpClient = new AmqpClient(channel);
                
                await amqpClient.Channel.ExchangeDeclareAsync(exchange: "myExchange", type: ExchangeType.Fanout);

                var prop = new BasicProperties();

                var queueName = (await amqpClient.Channel.QueueDeclareAsync()).QueueName;
                await amqpClient.Channel.QueueBindAsync(queue: queueName, exchange: "myExchange", 
                    routingKey: string.Empty);
                
                amqpClient.AddConsumer(queue: queueName, true);

                var publish = Step.Run("publish", ctx, async () =>
                    amqpClient.Publish(exchange: "myExchange", routingKey: string.Empty, prop, body: payload));

                var receive = Step.Run("receive", ctx, async () => await amqpClient.Receive());
                
                return Response.Ok();
            })
        .WithoutWarmUp()
        .WithLoadSimulations(
            Simulation.KeepConstant(1, TimeSpan.FromSeconds(30))
        );
        
        NBomberRunner
            .RegisterScenarios(scenario)
            .Run();
        
        connection.Close();
    }
}