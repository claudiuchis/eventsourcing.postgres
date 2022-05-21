using System.Threading.Tasks;
using Eventuous.Subscriptions;
using Eventuous.Subscriptions.Context;

public class TestEventHandler : IEventHandler {

    public TestEventHandler() => DiagnosticName = "testing";
    public string DiagnosticName { get; }
    public ValueTask<EventHandlingStatus> HandleEvent(IMessageConsumeContext context) {
        return ValueTask.FromResult(EventHandlingStatus.Success);
    }
}