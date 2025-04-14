using NBomber.CSharp;
using IndependentActors;

new IndependentActorsExample().Run();

public class IndependentActorsExample
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