using Eventuous.Postgres.Subscriptions;
using Eventuous.Subscriptions.Filters;
using Eventuous.Subscriptions;
using Eventuous.Subscriptions.Context;
using Moq;

namespace Eventuous.Postgres.Test;

public class SubscriptionTest : IDisposable
{
    TestFixture fixture;
    public SubscriptionTest() {
        fixture = new TestFixture();
    }

    public void Dispose()
    {
        fixture.Dispose();
    } 

    //[Fact]
    public async Task SubscribeToAllStream()
    {
        // arrange
        var mock = new Mock<IEventHandler>();
        mock.Setup(handler => handler.HandleEvent(It.IsAny<IMessageConsumeContext>())).ReturnsAsync(EventHandlingStatus.Success);

        var consumePipe = new ConsumePipe();
        consumePipe.AddDefaultConsumer(new [] { mock.Object });
        var subscriptionId = "test-all-stream";

        var subscription = new AllStreamSubscription(
            fixture.ConnectionString, 
            subscriptionId, 
            fixture.CheckpointStore, 
            consumePipe, 
            "test");

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
        await fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, streamEvents, CancellationToken.None);

        Thread.Sleep(2000);        
        // assert
        mock.Verify(handler => handler.HandleEvent(It.IsAny<IMessageConsumeContext>()), Times.AtLeastOnce());
    }

    //[Fact]
    public async Task SubscribeToStream()
    {
        // arrange
        var mock = new Mock<IEventHandler>();
        mock.Setup(handler => handler.HandleEvent(It.IsAny<IMessageConsumeContext>())).ReturnsAsync(EventHandlingStatus.Success);

        var consumePipe = new ConsumePipe();
        consumePipe.AddDefaultConsumer(new [] { mock.Object });
        var streamId = Guid.NewGuid().ToString();
        var stream = new StreamName(streamId);
        var subscriptionId = streamId;

        var subscription = new StreamSubscription(
            fixture.ConnectionString, 
            stream, 
            subscriptionId, 
            fixture.CheckpointStore, 
            consumePipe,
            "test" 
        );

        var cancellationTokenSource = new CancellationTokenSource();

        await subscription.Subscribe(
            subscriptionId => {},
            (subscriptionId, dropReason, exception) => {}, 
            cancellationTokenSource.Token);

        object[] events = new object[] {new AccountCreated(Guid.NewGuid().ToString()), new AccountCredited(100), new AccountDebited(50)};
        var position = 0;

        Thread.Sleep(2000);        

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

        Thread.Sleep(2000);        
        // assert
        mock.Verify(handler => handler.HandleEvent(It.IsAny<IMessageConsumeContext>()), Times.Exactly(3));
    }

}