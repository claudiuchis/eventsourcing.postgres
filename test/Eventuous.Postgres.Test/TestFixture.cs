using System;
using System.Data;
using Npgsql;
using Eventuous;
using Eventuous.Postgres.Store;
using Eventuous.Postgres.Schema;
using Eventuous.Subscriptions.Checkpoints;
using Eventuous.Postgres.Projections;
using Eventuous.Postgres.Subscriptions;
using Eventuous.Subscriptions.Filters; 

namespace Eventuous.Postgres.Test;

public class TestFixture: IDisposable {
    public TestFixture()
    {
        ConnectionString = "Host=localhost;Username=postgres;Password=changeme;Database=postgres";

        ConnStore = new NpgsqlConnection(ConnectionString);
        ConnStore.Open();
        ConnCheckpoint = new NpgsqlConnection(ConnectionString);
        ConnCheckpoint.Open();
        EventStoreOptions = new PostgresEventStoreOptions { SchemaName = "test"};
        EventStore = new PostgresEventStore(ConnStore, EventStoreOptions);
        AggregateStore = new AggregateStore(EventStore, EventStore);
        //SchemaSetup.Setup(Db, EventStoreOptions).Wait();
        CheckpointStore = new PostgresCheckpointStore(ConnCheckpoint, new PostgresCheckpointStoreOptions { SchemaName = "test"});

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