using Eventuous.Postgres.Subscriptions;
using Eventuous.Subscriptions.Filters;
using Eventuous.Subscriptions;
using Eventuous.Subscriptions.Context;
using Moq;

namespace Eventuous.Postgres.Test;

public class SubscriptionTest : IDisposable
{
    TestFixture fixture1;
    TestFixture fixture2;
    public SubscriptionTest() {
        fixture1 = new TestFixture();
        fixture2 = new TestFixture();
    }

    public void Dispose()
    {
        fixture1.Dispose();
        fixture2.Dispose();
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
        var subscription = new AllStreamSubscription(fixture1.Db, subscriptionId, fixture1.CheckpointStore, consumePipe, fixture1.EventStoreOptions);
        var cancellationTokenSource = new CancellationTokenSource();

        await subscription.Subscribe(
            subscriptionId => {},
            (subscriptionId, dropReason, exception) => {}, 
            cancellationTokenSource.Token);

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
        await fixture2.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, streamEvents, CancellationToken.None);

        Thread.Sleep(2000);        
        // assert
        mock.Verify(handler => handler.HandleEvent(null), Times.AtLeastOnce());
    }

}