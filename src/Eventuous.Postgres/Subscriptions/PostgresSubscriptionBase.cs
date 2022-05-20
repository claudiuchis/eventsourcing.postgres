using System;
using System.Linq;
using System.Data;
using System.Collections;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime.Serialization;
using Eventuous.Subscriptions;
using Eventuous.Subscriptions.Checkpoints;
using Eventuous.Subscriptions.Filters;
using Eventuous.Subscriptions.Context;
using Dapper;
using Eventuous.Postgres.Store;

namespace Eventuous.Postgres.Subscriptions;

public abstract class PostgresSubscriptionBase<T> : EventSubscription<T>
    where T: PostgresSubscriptionOptions
{
    readonly IDbConnection _conn;
    readonly ICheckpointStore _checkpointStore;
    readonly Thread _workerThread;
    readonly CancellationTokenSource _threadCancellationToken;
    readonly PostgresSubscriptionOptions _options;
    private bool doContinue { get => !_threadCancellationToken.Token.IsCancellationRequested; }
    private Checkpoint _lastCheckpoint;
    protected Checkpoint LastCheckpoint { 
        get => _lastCheckpoint; 
    }
    protected PostgresSubscriptionBase(
        IDbConnection       conn,
        ICheckpointStore    checkpointStore,
        ConsumePipe         consumePipe,
        T options
    ) : base(
        options,
        consumePipe
    ) 
    {
        _conn = conn;
        _options = options;
        _checkpointStore = checkpointStore;
        _threadCancellationToken = new CancellationTokenSource();
        _workerThread = new Thread(new ThreadStart(DoWork));
        _lastCheckpoint = new Checkpoint(_options.SubscriptionId, 0);
    }

    protected override async ValueTask Subscribe(CancellationToken cancellationToken) {
        _lastCheckpoint = await _checkpointStore.GetLastCheckpoint(Options.SubscriptionId, cancellationToken);
        _workerThread.Start();
    }

    protected override async ValueTask Unsubscribe(CancellationToken cancellationToken) {
        _threadCancellationToken.Cancel();
    }

    private async Task HandleEvent(
        PersistedEvent pe,
        CancellationToken ct
    )
        => await Handler(CreateContext(pe, ct)).NoContext();

    private async void DoWork() {

        var cancellationToken = _threadCancellationToken.Token;

        while (doContinue) {

            var result = await FetchData(cancellationToken);
            if (!result)
            {
                Task.Delay(1000).Wait();
            }
        }
    }

    protected virtual string GetQuery() => default;

    private async Task<bool> FetchData(CancellationToken cancellationToken) {
        var sql = GetQuery();

        var persistedEvents = await _conn.QueryAsync<PersistedEvent>(sql);

        if (!persistedEvents.Any()) return false;
        
        foreach(var evt in persistedEvents) {
            await HandleEvent(evt, cancellationToken);
            var checkpoint = new Checkpoint(Options.SubscriptionId, (ulong)evt.globalPosition);
            await _checkpointStore.StoreCheckpoint(checkpoint, false, cancellationToken); 
        };

        return true;
    }

    private IMessageConsumeContext CreateContext(PersistedEvent persistedEvent, CancellationToken cancellationToken) {
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

