A simplistic implementation of an event store using SQL databases based on the eventuous library for .NET (https://github.com/Eventuous/eventuous). 
Subscriptions (all stream, single stream) are done using polling.
As of now it support only Postgresql, but it can be easily modified to support any SQL database.

The events are stored in a single table, indexed by stream and gobal position.

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

For subscriptions, checkpoints are stored in a checkpoints table:

    CREATE TABLE IF NOT EXISTS checkpoints (
        id varchar,
        position bigint,
        PRIMARY KEY(id)
    );

There are tests that cover the event store, the aggregate store and subscriptions (single stream, all stream).