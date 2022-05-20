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

public class AllStreamSubscription : PostgresSubscriptionBase<AllStreamSubscriptionOptions> 
{
    public AllStreamSubscription(
        IDbConnection       conn,
        string              subscriptionId,
        ICheckpointStore    checkpointStore,
        ConsumePipe         consumePipe,
        string              schemaName = "public"
    ) : base(
        conn,
        checkpointStore,
        consumePipe,
        new AllStreamSubscriptionOptions {
            SubscriptionId = subscriptionId,
            SchemaName = schemaName
        }
    ) {}

    protected override string GetQuery() {
        return $@"
            SELECT eventId, eventType, stream, streamPosition, globalPosition, payload, metadata, created
            FROM {Options.SchemaName}.events
            WHERE globalPosition > {LastCheckpoint.Position} 
            ORDER BY globalPosition ASC
            LIMIT {Options.BatchCount}
        ";
    }
}

