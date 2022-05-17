using System;
using System.Linq;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime.Serialization;
using Eventuous.Subscriptions;
using Eventuous.Subscriptions.Checkpoints;
using Eventuous.Subscriptions.Filters;
using Eventuous.Subscriptions.Context;
using Dapper;
using Eventuous.Postgres.Store;
using Npgsql;

namespace Eventuous.Postgres.Subscriptions;

public class AllStreamSubscription : EventSubscription<AllStreamSubscriptionOptions> 
{
    readonly IDbConnection _conn;
    readonly ICheckpointStore _checkpointStore;
    readonly Thread _workerThread;
    readonly CancellationTokenSource _threadCancellationToken;
    readonly PostgresEventStoreOptions _eventStoreOptions;
    private bool doContinue { get => !_threadCancellationToken.Token.IsCancellationRequested; }
    public AllStreamSubscription(
        IDbConnection       conn,
        string              subscriptionId,
        ICheckpointStore    checkpointStore,
        ConsumePipe         consumePipe,
        PostgresEventStoreOptions options
    ) : base(
        new AllStreamSubscriptionOptions {
            SubscriptionId = subscriptionId
        },
        consumePipe
    ) 
    {
        _conn = conn;
        _eventStoreOptions = options;
        _checkpointStore = checkpointStore;
        _threadCancellationToken = new CancellationTokenSource();
        _workerThread = new Thread(new ThreadStart(DoWork));
    }

    protected override async ValueTask Subscribe(CancellationToken cancellationToken) {
        _workerThread.Start();
    }

    protected override async ValueTask Unsubscribe(CancellationToken cancellationToken) {
        _threadCancellationToken.Cancel();
        var sql = $"UNLISTEN {_eventStoreOptions.SchemaName}.events_table";
        await _conn.ExecuteAsync(sql);
    }

    private async void DoWork() {

        var cancellationToken = _threadCancellationToken.Token;

        if (doContinue) await Catchup(cancellationToken);

        if (doContinue) await Listen(cancellationToken);
    }
    private async Task Catchup(CancellationToken cancellationToken) 
    {
        while(doContinue && await FetchData(cancellationToken)) {}
    }

    private async Task Listen(CancellationToken cancellationToken) {

        ((NpgsqlConnection)_conn).Notification += async (o, e) => await FetchData(cancellationToken); 

        var sql = $"LISTEN {_eventStoreOptions.SchemaName}.events_table";
        using (var cmd = new NpgsqlCommand(sql, (NpgsqlConnection)_conn)) 
        {
            await cmd.ExecuteNonQueryAsync();
        }

        while (doContinue) {
            ((NpgsqlConnection)_conn).Wait(); 
        }        
    }

    private async Task<bool> FetchData(CancellationToken cancellationToken) {

        var checkpoint = await _checkpointStore.GetLastCheckpoint(Options.SubscriptionId, cancellationToken);

        var sql = $@"
            SELECT eventId, eventType, stream, streamPosition, globalPosition, payload, metadata, created
            FROM {_eventStoreOptions.SchemaName}.events
            WHERE globalPosition > {checkpoint.Position} 
            ORDER BY globalPosition ASC
            LIMIT {Options.BatchCount}
        ";

        var persistedEvents = await _conn.QueryAsync<PersistedEvent>(sql);

        if (!persistedEvents.Any()) return false;
        
        persistedEvents.Select( async (evt) => await HandleEvent(evt, cancellationToken));

        return true;

        async Task HandleEvent(
            PersistedEvent pe,
            CancellationToken ct
        )
            => await Handler(CreateContext(pe, ct)).NoContext();
    }

    IMessageConsumeContext CreateContext(PersistedEvent persistedEvent, CancellationToken cancellationToken) {
        var streamEvent = persistedEvent.AsStreamEvent();
        return new MessageConsumeContext(
            persistedEvent.eventId,
            persistedEvent.eventType,
            streamEvent.ContentType,
            persistedEvent.stream,
            persistedEvent.streamPosition,
            (ulong)persistedEvent.globalPosition,
            _sequence++,
            persistedEvent.created,
            streamEvent.Payload,
            streamEvent.Metadata,
            Options.SubscriptionId,
            cancellationToken
        );
    }

    ulong _sequence;

}

