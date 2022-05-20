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
using Npgsql;

namespace Eventuous.Postgres.Subscriptions;

public abstract class PostgresSubscriptionBase<T> : EventSubscription<T>
    where T: PostgresSubscriptionOptions
{
    readonly NpgsqlConnection _fetchConn;
    readonly NpgsqlConnection _notifyConn;
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
        string              connectionString,
        ICheckpointStore    checkpointStore,
        ConsumePipe         consumePipe,
        T options
    ) : base(
        options,
        consumePipe
    ) 
    {
        _fetchConn = new NpgsqlConnection(connectionString);
        _fetchConn.Open();
        _notifyConn = new NpgsqlConnection(connectionString);
        _notifyConn.Open();
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
        var sql = $"UNLISTEN events_table";
        await _fetchConn.ExecuteAsync(sql);
    }

    protected async ValueTask StoreCheckpoint(Checkpoint checkpoint, CancellationToken cancellationToken)
    {
        await _checkpointStore.StoreCheckpoint(checkpoint, false, cancellationToken);        
    }

    protected async Task HandleEvent(
        PersistedEvent pe,
        CancellationToken ct
    )
        => await Handler(CreateContext(pe, ct)).NoContext();

    protected async ValueTask<PersistedEvent[]> FetchQuery(string sql)
        => (await _fetchConn.QueryAsync<PersistedEvent>(sql)).ToArray();

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

        _notifyConn.Notification += async (o, e) => await FetchData(cancellationToken); 

        var sql = $"LISTEN events_table";
        using (var cmd = new NpgsqlCommand(sql, _notifyConn)) 
        {
            await cmd.ExecuteNonQueryAsync();
        }

        while (doContinue) {
            _notifyConn.Wait(); 
        }        
    }

    protected virtual Task<bool> FetchData(CancellationToken cancellationToken) => default;

    protected IMessageConsumeContext CreateContext(PersistedEvent persistedEvent, CancellationToken cancellationToken) {
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

