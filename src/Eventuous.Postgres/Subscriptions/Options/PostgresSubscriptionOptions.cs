using Eventuous.Subscriptions;

namespace Eventuous.Postgres.Subscriptions;

public record PostgresSubscriptionOptions : SubscriptionOptions {
    public int BatchCount { get; init; } = 100;
    public string SchemaName { get; init; } = "public";
}