using NBomber.CSharp;
using IndependentActors;

new PingPongAmqpTest().Run();

public class PingPongAmqpTest
{
    public void Run()
    {
        NBomberRunner.RegisterScenarios(
            new PublishScenario().Create("1"), 
            new PublishScenario().Create("2"), 
            new ConsumeScenario().Create()
        )
        .Run();
    }
}