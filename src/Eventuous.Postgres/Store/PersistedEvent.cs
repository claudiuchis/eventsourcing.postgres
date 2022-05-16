namespace Eventuous.Postgres.Store;
public record PersistedEvent(
    string eventId, 
    string eventType, 
    string stream, 
    int streamPosition, 
    string payload, 
    string metadata
);
