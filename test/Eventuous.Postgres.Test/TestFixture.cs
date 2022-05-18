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
        var connString = "Host=localhost;Username=postgres;Password=changeme;Database=postgres";

        Db = new NpgsqlConnection(connString);
        Db.Open();
        EventStoreOptions = new PostgresEventStoreOptions { SchemaName = "test"};
        EventStore = new PostgresEventStore(Db, EventStoreOptions);
        SchemaSetup.Setup(Db, EventStoreOptions).Wait();
        CheckpointStore = new PostgresCheckpointStore(Db, new PostgresCheckpointStoreOptions { SchemaName = "test"});

        TypeMap.AddType<AccountCreated>("AccountCreated");
        TypeMap.AddType<AccountCredited>("AccountCredited");
        TypeMap.AddType<AccountDebited>("AccountDebited");
    }

    public void Dispose()
    {
        Db.Close();
    }

    public NpgsqlConnection Db { get; private set; }
    public PostgresEventStore EventStore { get; private set; }
    public ICheckpointStore CheckpointStore { get; private set; }
    public PostgresEventStoreOptions EventStoreOptions { get; private set; }
}