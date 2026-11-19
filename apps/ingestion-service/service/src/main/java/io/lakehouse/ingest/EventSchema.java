package io.lakehouse.ingest;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.HashMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/** The Avro schema of an event and the mapping from a validated request to a record. */
public final class EventSchema {

    private final String json;
    private final Schema schema;
    private final Clock clock;

    public EventSchema(Clock clock) {
        this(clock, Config.DEFAULT_SCHEMA_RESOURCE);
    }

    public EventSchema(Clock clock, String resource) {
        this.clock = clock;
        try (InputStream in = EventSchema.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("missing resource " + resource);
            }
            this.json = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.schema = new Schema.Parser().parse(json);
    }

    public Schema schema() {
        return schema;
    }

    /** The schema as registered with the registry (canonical text of the .avsc file). */
    public String json() {
        return json;
    }

    public GenericRecord toRecord(EventRequest request, String eventId) {
        long now = clock.millis();
        GenericRecord record = new GenericData.Record(schema);
        record.put("event_id", eventId);
        record.put("source", request.source());
        record.put("type", request.type());
        record.put("event_time", request.timestamp() != null ? request.timestamp() : now);
        record.put("ingest_time", now);
        record.put("value", request.value());
        if (schema.getField("region") != null) { // absent when simulating a schema v1 producer
            record.put("region", request.region());
        }
        record.put("attributes", request.attributes() != null ? request.attributes() : new HashMap<String, String>());
        return record;
    }
}
