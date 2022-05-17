using System;
namespace Eventuous.Postgres.Store;
public record PersistedEvent(
    string eventId, 
    string eventType, 
    string stream, 
    int streamPosition, 
    long globalPosition,
    string payload, 
    string metadata,
    DateTime created
);
