using System;
using System.Threading.Tasks;
using Eventuous.Postgres.Store;
using Npgsql;
using System.Linq;

namespace Eventuous.Postgres.Test;

public class EventStoreTest : IDisposable
{
    TestFixture fixture;
    public EventStoreTest() {
        fixture = new TestFixture();
    }

    public void Dispose()
    {
        fixture.Dispose();
    } 

    [Fact]
    public async Task StoreEvents()
    {
        // arrange
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
        var persistedEvents = await fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 10, CancellationToken.None);

        // assert
        Assert.Equal(3, persistedEvents.Count());
    }

    [Fact]
    public async Task ThrowsExceptionWhenWrongVersion()
    {
        // arrange
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

        position = 0;
        StreamEvent[] streamEvents2 = events.Select( evt => 
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
        await Assert.ThrowsAsync<WrongExpectedVersionException>(async () => await fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, streamEvents2, CancellationToken.None));
    }

}