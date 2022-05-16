using Eventuous.Postgres.Subscriptions;
using Eventuous.Subscriptions.Filters;
using Eventuous.Subscriptions;
using Eventuous.Subscriptions.Context;
using Moq;

namespace Eventuous.Postgres.Test;

public class SubscriptionTest : IClassFixture<TestFixture>
{
    TestFixture fixture;
    public SubscriptionTest(TestFixture fixture) {
        this.fixture = fixture;
    }

    //[Fact]
    public async Task SubscribeToAllStream()
    {
        // arrange
        var mock = new Mock<IEventHandler>();
        mock.Setup(handler => handler.HandleEvent(null)).ReturnsAsync(EventHandlingStatus.Success);

        var consumePipe = new ConsumePipe();
        consumePipe.AddDefaultConsumer(new [] { mock.Object });
        var subscriptionId = "test-all-stream";
        var subscription = new AllStreamSubscription(fixture.Db, subscriptionId, fixture.CheckpointStore, consumePipe);

        var stream = new StreamName(Guid.NewGuid().ToString());
        object[] events = new object[] {new AccountCreated(Guid.NewGuid().ToString()), new AccountCredited(100), new AccountDebited(50)};
        var position = 0;

        StreamEvent[] streamEvents = events.Select( evt => 
            new StreamEvent (
                Guid.NewGuid(),
                evt,
                new Metadata(),
                "application/json",
                ++position
        )).ToArray();

        // act
        await fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, streamEvents, CancellationToken.None);
        
        // assert
        mock.Verify(handler => handler.HandleEvent(null), Times.AtLeastOnce());
    }

}