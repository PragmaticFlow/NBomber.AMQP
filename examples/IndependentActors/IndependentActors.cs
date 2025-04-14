using NBomber.CSharp;
using IndependentActors;

new PingPongAmqpTest().Run();

public class PingPongAmqpTest
{
    public void Run()
    {
        NBomberRunner.RegisterScenarios(
            new PublishScenario().Create(), 
            new ConsumeScenario().Create()
        )
        .Run();
    }
}