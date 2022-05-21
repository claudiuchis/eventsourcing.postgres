using Npgsql;
using Eventuous.Postgres.Store;
using Eventuous.Postgres.Schema;
using Eventuous.Subscriptions.Checkpoints;
using Eventuous.Postgres.Projections;

namespace Eventuous.Postgres.Test;

public class TestFixture: IDisposable {
    public TestFixture()
    {
        ConnectionString = "Host=localhost;Username=postgres;Password=changeme;Database=postgres";
        var schema = "test";

        ConnStore = new NpgsqlConnection(ConnectionString);
        ConnStore.Open();
        ConnCheckpoint = new NpgsqlConnection(ConnectionString);
        ConnCheckpoint.Open();
        EventStoreOptions = new PostgresEventStoreOptions { SchemaName = schema};
        EventStore = new PostgresEventStore(ConnStore, EventStoreOptions);
        AggregateStore = new AggregateStore(EventStore, EventStore);
        CheckpointStore = new PostgresCheckpointStore(ConnCheckpoint, new PostgresCheckpointStoreOptions { SchemaName = schema});

        // create the database schema
        SchemaSetup.Setup(ConnStore, schema).Wait();

        TypeMap.AddType<AccountCreated>("AccountCreated");
        TypeMap.AddType<AccountCredited>("AccountCredited");
        TypeMap.AddType<AccountDebited>("AccountDebited");
    }

    public void Dispose()
    {
        ConnStore.Close();
        ConnCheckpoint.Close();
    }

    public NpgsqlConnection ConnStore { get; private set; }
    public NpgsqlConnection ConnCheckpoint { get; private set; }
    public PostgresEventStore EventStore { get; private set; }
    public ICheckpointStore CheckpointStore { get; private set; }
    public PostgresEventStoreOptions EventStoreOptions { get; private set; }
    public AggregateStore AggregateStore { get; private set; }
    public string ConnectionString;
}

[CollectionDefinition("Test collection")]
public class DatabaseCollection : ICollectionFixture<TestFixture>
{
    // This class has no code, and is never created. Its purpose is simply
    // to be the place to apply [CollectionDefinition] and all the
    // ICollectionFixture<> interfaces.
}