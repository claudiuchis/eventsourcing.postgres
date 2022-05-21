using System.Data;
using Eventuous.Postgres.Subscriptions;
using Eventuous.Subscriptions.Filters;
using Eventuous.Subscriptions;
using Eventuous.Subscriptions.Context;
using Eventuous.Subscriptions.Checkpoints;
using Moq;
using Npgsql;

namespace Eventuous.Postgres.Test;
[Collection("Test collection")]
public class SubscriptionTest : IDisposable
{
    TestFixture fixture;
    IDbConnection conn;
    public SubscriptionTest(TestFixture fixture) {
        this.fixture = fixture;
        conn = new NpgsqlConnection(fixture.ConnectionString);
        conn.Open();
    }

    public void Dispose()
    {
        fixture.Dispose();
    } 

    //[Fact]
    public async Task CheckpointStoreTest() {
        // arrange
        var checkpoint = new Checkpoint("test-checkpoint", 20);
        // act 
        await fixture.CheckpointStore.StoreCheckpoint(checkpoint, false, CancellationToken.None);
        var storedCheckpoint = await fixture.CheckpointStore.GetLastCheckpoint(checkpoint.Id, CancellationToken.None);
        // assert
        Assert.Equal(checkpoint.Position, storedCheckpoint.Position);
    }

    [Fact]
    public async Task AllStreamSubscription_Test()
    {
        // arrange
        var mock = new Mock<IEventHandler>();
        mock.Setup(handler => handler.HandleEvent(It.IsAny<IMessageConsumeContext>())).ReturnsAsync(EventHandlingStatus.Success);

        var consumePipe = new ConsumePipe();
        consumePipe.AddDefaultConsumer(new [] { mock.Object });
        var subscriptionId = "test-all-stream";

        var subscription = new AllStreamSubscription(
            conn, 
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
        Thread.Sleep(1000);        
        await subscription.Unsubscribe(subscriptionId => {}, CancellationToken.None);        
        Thread.Sleep(1000);

        // assert
        mock.Verify(handler => handler.HandleEvent(It.IsAny<IMessageConsumeContext>()), Times.AtLeastOnce());
    }

    [Fact]
    public async Task StreamSubscription_Test()
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
            conn, 
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

        Thread.Sleep(100);        

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
        Thread.Sleep(1000);
        await subscription.Unsubscribe(subscriptionId => {}, CancellationToken.None);        
        Thread.Sleep(1000);

        // assert
        mock.Verify(handler => handler.HandleEvent(It.IsAny<IMessageConsumeContext>()), Times.Exactly(3));
    }

}