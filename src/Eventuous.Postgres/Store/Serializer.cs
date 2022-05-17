using System;
using System.Text;
using System.Runtime.Serialization;

namespace Eventuous.Postgres.Store;

public static class Serializer {
    public static StreamEvent AsStreamEvent(this PersistedEvent persistedEvent) {
            var serializer = DefaultEventSerializer.Instance; 
            var metaSerializer = DefaultMetadataSerializer.Instance;

            var contentType = ((DefaultEventSerializer)serializer).ContentType;
            var deserialized = serializer.DeserializeEvent(Encoding.UTF8.GetBytes(persistedEvent.payload), persistedEvent.eventType, contentType);

            return deserialized switch {
                SuccessfullyDeserialized success => PayloadAsStreamEvent(success.Payload),
                FailedToDeserialize failed => throw new SerializationException(
                    $"Can't deserialize {persistedEvent.eventType}: {failed.Error}"
                ),
                _ => throw new Exception("Unknown deserialization result")
            };

            StreamEvent PayloadAsStreamEvent(object payload)
                => new(
                    Guid.Parse(persistedEvent.eventId),
                    payload,
                    metaSerializer.Deserialize(Encoding.UTF8.GetBytes(persistedEvent.metadata)) ?? new Metadata(),
                    contentType,
                    persistedEvent.streamPosition
                );
    }

    public static PersistedEvent AsPersistedEvent(this StreamEvent streamEvent, string stream) {
        var serializer = DefaultEventSerializer.Instance; 
        var metaSerializer = DefaultMetadataSerializer.Instance;
        var (eventType, contentType, payload) = serializer.SerializeEvent(streamEvent.Payload!);
        return new(
            streamEvent.Id.ToString(),
            eventType,
            stream,
            (int)streamEvent.Position + 1,
            (int)streamEvent.Position + 1,
            Encoding.UTF8.GetString(payload),
            Encoding.UTF8.GetString(metaSerializer.Serialize(streamEvent.Metadata)),
            DateTime.UtcNow
        );
    }

}
