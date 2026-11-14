package io.lakehouse.ingest;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

/** JSON body of {@code POST /v1/events}. Unknown fields are rejected. */
public record EventRequest(
        @JsonProperty("event_id") String eventId,
        @JsonProperty("source") String source,
        @JsonProperty("type") String type,
        @JsonProperty("timestamp") Long timestamp,
        @JsonProperty("value") Double value,
        @JsonProperty("attributes") Map<String, String> attributes,
        @JsonProperty("region") String region) {

    /** Events without a region (schema v1 clients). */
    public EventRequest(String eventId, String source, String type, Long timestamp, Double value,
            Map<String, String> attributes) {
        this(eventId, source, type, timestamp, value, attributes, null);
    }
}
