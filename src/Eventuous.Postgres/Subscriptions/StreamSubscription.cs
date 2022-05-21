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
        IDbConnection       conn,
        StreamName          streamName,
        string              subscriptionId,
        ICheckpointStore    checkpointStore,
        ConsumePipe         consumePipe,
        string              schemaName = "public"
    ) : base(
        conn,
        checkpointStore,
        consumePipe,
        new StreamSubscriptionOptions {
            SubscriptionId = subscriptionId,
            StreamName = streamName,
            SchemaName = schemaName
        }
    ) {}

    protected override string GetQuery() {

        return $@"
            SELECT eventId, eventType, stream, streamPosition, globalPosition, payload, metadata, created
            FROM {Options.SchemaName}.events
            WHERE stream = '{Options.StreamName}'  
            AND globalPosition > {LastCheckpoint.Position} 
            ORDER BY globalPosition ASC
            LIMIT {Options.BatchCount}
        ";
    }
}

