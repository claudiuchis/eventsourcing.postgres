using System;
using System.Linq;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime.Serialization;
using Eventuous.Subscriptions;
using Eventuous.Subscriptions.Checkpoints;
using Eventuous.Subscriptions.Filters;
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

    private bool doContinue { get => !_threadCancellationToken.Token.IsCancellationRequested; }
    public AllStreamSubscription(
        IDbConnection       conn,
        string              subscriptionId,
        ICheckpointStore    checkpointStore,
        ConsumePipe         consumePipe
    ) : base(
        new AllStreamSubscriptionOptions {
            SubscriptionId = subscriptionId
        },
        consumePipe
    ) 
    {
        _conn = conn;
        _checkpointStore = checkpointStore;
        _threadCancellationToken = new CancellationTokenSource();
        _workerThread = new Thread(new ThreadStart(DoWork));
    }

    protected override async ValueTask Subscribe(CancellationToken cancellationToken) {
        _workerThread.Start();
    }

    protected override async ValueTask Unsubscribe(CancellationToken cancellationToken) {
        _threadCancellationToken.Cancel();
        await _conn.ExecuteAsync("UNLISTEN events_table");
    }

    private async void DoWork() {
        if (doContinue) await Catchup();
        if (doContinue) await Listen();
    }
    private async Task Catchup() 
    {
        while(doContinue && await FetchData()) {}
    }

    private async Task Listen() {
        ((NpgsqlConnection)_conn).Notification += async (o, e) => await FetchData(); 
        using (var cmd = new NpgsqlCommand("LISTEN events_table", (NpgsqlConnection)_conn)) 
        {
            await cmd.ExecuteNonQueryAsync();
        }
        while (doContinue) {
            ((NpgsqlConnection)_conn).Wait(); 
        }        
    }

    private async Task<bool> FetchData() {
        var checkpoint = await _checkpointStore.GetLastCheckpoint(Options.SubscriptionId, _threadCancellationToken.Token);
        var sql = $@"
            SELECT eventId, eventType, stream, streamPosition, globalPosition, payload, metadata, created
            FROM {Options.SchemaName}.events
            WHERE globalPosition > {checkpoint.Position} 
            ORDER BY globalPosition ASC
            LIMIT {Options.BatchCount}
        ";
        var persistedEvents = await _conn.QueryAsync<PersistedEvent>(sql);
        if (!persistedEvents.Any()) return false;
        
        foreach(PersistedEvent persistedEvent in persistedEvents)
        {
            var streamEvent = persistedEvent.AsStreamEvent();
            // TODO: notify subscribers
        }
        return true;
    }
}

