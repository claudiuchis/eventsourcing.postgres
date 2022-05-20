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

public class StreamSubscription : PostgresSubscriptionBase<StreamSubscriptionOptions> 
{
    public StreamSubscription(
        string              connectionString,
        StreamName          streamName,
        string              subscriptionId,
        ICheckpointStore    checkpointStore,
        ConsumePipe         consumePipe,
        string              schemaName = "public"
    ) : base(
        connectionString,
        checkpointStore,
        consumePipe,
        new StreamSubscriptionOptions {
            SubscriptionId = subscriptionId,
            StreamName = streamName,
            SchemaName = schemaName
        }
    ) {}

    protected override async Task<bool> FetchData(CancellationToken cancellationToken) {

        var sql = $@"
            SELECT eventId, eventType, stream, streamPosition, globalPosition, payload, metadata, created
            FROM {Options.SchemaName}.events
            WHERE stream = '{Options.StreamName}'  
            AND streamPosition > {LastCheckpoint.Position} 
            ORDER BY streamPosition ASC
            LIMIT {Options.BatchCount}
        ";
        var persistedEvents = await FetchQuery(sql);

        if (!persistedEvents.Any()) return false;
        
        foreach(var evt in persistedEvents) {
            await HandleEvent(evt, cancellationToken);
            var checkpoint = new Checkpoint(Options.SubscriptionId, (ulong)evt.globalPosition);
            await StoreCheckpoint(checkpoint, cancellationToken);
        };

        return true;
    }
}

