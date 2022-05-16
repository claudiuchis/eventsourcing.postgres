namespace Eventuous.Postgres.Store;

public record PostgresEventStoreOptions {
    public string SchemaName { get; init; } = "public";
}