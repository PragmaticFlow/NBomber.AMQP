using RabbitMQ.Client;
using NBomber.AMQP;
using NBomber.CSharp;
using NBomber.Data;
using NBomber;

namespace Tests.AMQP;

public class AmqpTest
{
    [Fact]
    public void EndToEnd()
    {
        var clientPool = new ClientPool<AmqpClient>();
        var message = Data.GenerateRandomBytes(200);

        var scenario = Scenario.Create("client_pool_scenario", async ctx =>
        {
            // get a client from the pool by Scenario InstanceID
            var client = clientPool.GetClient(ctx.ScenarioInfo);

            var publish = await Step.Run("publish", ctx, async () =>
            {
                var queueName = $"queue_{ctx.ScenarioInfo.InstanceNumber}";

                var response = await client.Publish(exchange: "myExchange", routingKey: queueName, message);
                return response;
            });

            var receive = await Step.Run("receive", ctx, async () =>
            {
                // pass the ScenarioCancellationToken to stop waiting for a response if the scenario finish event is triggered
                var response = await client.Receive(ctx.ScenarioCancellationToken);
                return response;
            });

            return Response.Ok();
        })
        .WithWarmUpDuration(TimeSpan.FromSeconds(5))
        .WithLoadSimulations(Simulation.KeepConstant(10, TimeSpan.FromSeconds(5)))
        .WithInit(async context =>
        {
            var factory = new ConnectionFactory { HostName = "localhost" };

            // initialize a client and add it to the ClientPool
            for (var i = 0; i < 100; i++)
            {
                var connection = await factory.CreateConnectionAsync();
                var channel = await connection.CreateChannelAsync();
                var amqpClient = new AmqpClient(channel);

                var queueName = $"queue_{i}";

                var result = await amqpClient.DeclareQueue(exchange: "myExchange", exchangeType: ExchangeType.Direct, queue: queueName,
                        routingKey: queueName);

                if (!result.IsError)
                {
                    await amqpClient.Subscribe(queue: queueName);
                    clientPool.AddClient(amqpClient);
                }
                else
                    throw new Exception("client can't connect to the AMQP broker");

                await Task.Delay(10);
            }
        })
        .WithClean(ctx =>
        {
            clientPool.DisposeClients(client => client.Dispose());
            return Task.CompletedTask;
        });

        var stats = NBomberRunner
            .RegisterScenarios(scenario)
            .Run();
        
        Assert.True(stats.AllOkCount > 0);
        Assert.True(stats.AllFailCount == 0);

        foreach (var scenarioStats in stats.ScenarioStats)
        {
            foreach (var stepStats in scenarioStats.StepStats)
                Assert.True(stepStats.Ok.Latency.MaxMs > 0);
        }
    }
}