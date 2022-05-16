using System;
using System.Text;
using System.Data;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using System.Runtime.Serialization;
using Eventuous;
using Dapper;

namespace Eventuous.Postgres.Store;

public class PostgresEventStore: IEventReader, IEventWriter {

    readonly IDbConnection _conn;
    readonly PostgresEventStoreOptions _options;
    readonly IEventSerializer           _serializer = DefaultEventSerializer.Instance;
    readonly IMetadataSerializer        _metaSerializer = DefaultMetadataSerializer.Instance;
    public PostgresEventStore(
        IDbConnection               conn, 
        PostgresEventStoreOptions?  options = null
    ) {
        _conn       = conn;
        _options    = options ?? new PostgresEventStoreOptions();
    }

    public async Task<AppendEventsResult> AppendEvents(
        StreamName stream,
        ExpectedStreamVersion expectedVersion,
        IReadOnlyCollection<StreamEvent> streamEvents,
        CancellationToken cancellationToken
    ) {

        var persistedEvents = streamEvents.Select(streamEvent => streamEvent.AsPersistedEvent(stream)).ToArray();
        var contentType = ((DefaultEventSerializer)_serializer).ContentType;

        var sql = $@"
            INSERT INTO {_options.SchemaName}.events (eventId, eventType, stream, streamPosition, payload, metadata) 
            VALUES (@eventId, @eventType, @stream, @streamPosition, @payload, @metadata)
        ";

        try {
            await _conn.ExecuteAsync(sql, persistedEvents);

        }
        catch( Exception e) {
            if (e.Message.Contains("events_stream_streamposition_key"))
                throw new WrongExpectedVersionException(stream);
            throw e;
        }

        return new AppendEventsResult(0, persistedEvents.Last().streamPosition + 1);

    }

    public async Task<StreamEvent[]> ReadEvents(
        StreamName stream,
        StreamReadPosition start,
        int count,
        CancellationToken cancellationToken
    ) {
        var sql = $@"
            SELECT eventId, eventType, stream, streamPosition, payload, metadata 
            FROM {_options.SchemaName}.events 
            WHERE stream = '{stream}' 
            AND streamPosition >= {start.Value} 
            ORDER BY streamPosition ASC
            LIMIT {count}; 
        ";
        var events = await _conn.QueryAsync<PersistedEvent>(sql);

        return events.Select(persistedEvent => persistedEvent.AsStreamEvent()).ToArray();
    }
}

