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
        var options = new PostgresEventStoreOptions { SchemaName = "test"};
        EventStore = new PostgresEventStore(Db, options);
        //SchemaSetup.Setup(Db, options).Wait();
        CheckpointStore = new PostgresCheckpointStore(Db, new PostgresCheckpointStoreOptions { SchemaName = "test"});

        TypeMap.AddType<AccountCreated>("AccountCreated");
        TypeMap.AddType<AccountCredited>("AccountCredited");
        TypeMap.AddType<AccountDebited>("AccountDebited");
    }

    public void Dispose()
    {

    }

    public IDbConnection Db { get; private set; }
    public PostgresEventStore EventStore { get; private set; }
    public ICheckpointStore CheckpointStore { get; private set; }
}