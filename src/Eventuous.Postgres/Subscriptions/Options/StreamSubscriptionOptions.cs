using Eventuous.Subscriptions;

namespace Eventuous.Postgres.Subscriptions;

public record StreamSubscriptionOptions : PostgresSubscriptionOptions {
    public StreamName StreamName { get; set; }
}