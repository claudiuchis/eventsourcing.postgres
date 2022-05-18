using System.Data;
using System.Threading.Tasks;
using Dapper;
using Eventuous.Postgres.Store;

namespace Eventuous.Postgres.Schema;

public static class SchemaSetup {
    public async static Task Setup(IDbConnection conn, PostgresEventStoreOptions options) {

        var sql = 
            $@"
            CREATE SCHEMA IF NOT EXISTS {options.SchemaName};
            SET search_path TO {options.SchemaName};

            -- create the events table
            CREATE TABLE IF NOT EXISTS events (
                eventId VARCHAR PRIMARY KEY,
                eventType VARCHAR NOT NULL,
                stream VARCHAR NOT NULL,
                streamPosition INT NOT NULL,
                globalPosition BIGSERIAL, 
                payload VARCHAR NOT NULL,
                metadata VARCHAR,
                created TIMESTAMP DEFAULT NOW(),
                UNIQUE(stream, streamPosition)
            );

            CREATE INDEX IF NOT EXISTS events_stream_idx ON events (stream);    
            CREATE INDEX IF NOT EXISTS events_globalposition_idx ON events (globalPosition);    

            CREATE TABLE IF NOT EXISTS checkpoints (
                id varchar,
                position bigint,
                PRIMARY KEY(id)
            );

            CREATE OR REPLACE FUNCTION fn_events_table_modified() RETURNS TRIGGER AS $psql$
            BEGIN
            PERFORM pg_notify(
                'events_table',
                ''
            );return new;
            end;$psql$ language plpgsql;
            
            CREATE OR REPLACE TRIGGER events_table_updated BEFORE
            INSERT
            ON events FOR EACH ROW EXECUTE procedure fn_events_table_modified();        
        ";
    
        await conn.ExecuteAsync(sql);

    }
}