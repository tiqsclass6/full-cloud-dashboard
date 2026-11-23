package io.lakehouse.flink;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * What the replay tool does with one Kafka offset, decided from facts it has looked up (pure, so it is unit-tested).
 * A replay is sent through the ingestion API with the original event_id and the attribute {@link #MARKER}, which
 * the reconciliation counts as accounting for the original offset.
 */
public final class Replay {

    /** Value {@code <partition>:<offset>:<kafka timestamp ms>} of the record this event replaces. */
    public static final String MARKER = "lakehouse.replayed_from";
    /** The API's limit (EventValidator.MAX_ATTRIBUTES). */
    static final int MAX_ATTRIBUTES = 32;
    /** Flink's de-duplication TTL starts when it processes a record, a little after the Kafka timestamp. */
    static final Duration TTL_MARGIN = Duration.ofMinutes(10);
    /** Avro fields the API takes back (event_time is sent as timestamp); ingest_time is set again by the API. */
    static final Set<String> API_FIELDS = Set.of("event_id", "source", "type", "event_time", "value", "region",
            "attributes", "ingest_time");

    private Replay() {}

    public enum Outcome {
        /** Send it again. */
        REPLAY,
        /** In events at this offset: nothing to do. */
        STORED,
        /** In events_skipped at this offset (a duplicate copy, or undecodable: use the undecodable mode). */
        SKIPPED,
        /** An earlier replay of this offset is in events. */
        ALREADY_REPLAYED,
        /** The event_id is in events at another offset (e.g. a later retry from the source). */
        EVENT_ID_STORED,
        /** Gone from Kafka (retention) or beyond its end. */
        NOT_IN_KAFKA,
        /** The bytes still cannot be decoded. */
        UNDECODABLE,
        /** Flink may still hold the id in its de-duplication state and would drop the copy: retry later. */
        WAIT_FOR_DEDUPE_TTL,
        /** The record cannot be sent through the API unchanged. */
        NOT_SENDABLE
    }

    public record Decision(Outcome outcome, String detail) {
        static Decision of(Outcome outcome) {
            return new Decision(outcome, null);
        }
    }

    /** Facts about one offset; {@code record} is null when it could not be decoded ({@code decodeError}). */
    public record Facts(int partition, long offset, long kafkaTimestampMs, boolean inEvents, String skippedReason,
            boolean alreadyReplayed, String notInKafka, GenericRecord record, String decodeError,
            boolean eventIdStored) {}

    public static String marker(int partition, long offset, long kafkaTimestampMs) {
        return partition + ":" + offset + ":" + kafkaTimestampMs;
    }

    /** The partition and offset of a marker value, or null if it is not one. */
    public static long[] parseMarker(String value) {
        if (value == null) {
            return null;
        }
        String[] parts = value.split(":");
        if (parts.length != 3) {
            return null;
        }
        try {
            return new long[] {Integer.parseInt(parts[0]), Long.parseLong(parts[1]), Long.parseLong(parts[2])};
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** A Kafka offset the reconciliation reported missing. */
    public static Decision decideMissing(Facts f, Instant now, Duration dedupeTtl) {
        if (f.inEvents()) {
            return Decision.of(Outcome.STORED);
        }
        if (f.skippedReason() != null) {
            return new Decision(Outcome.SKIPPED, f.skippedReason());
        }
        if (f.alreadyReplayed()) {
            return Decision.of(Outcome.ALREADY_REPLAYED);
        }
        if (f.notInKafka() != null) {
            return new Decision(Outcome.NOT_IN_KAFKA, f.notInKafka());
        }
        Decision common = decideRecord(f);
        if (common != null) {
            return common;
        }
        Instant safe = Instant.ofEpochMilli(f.kafkaTimestampMs()).plus(dedupeTtl).plus(TTL_MARGIN);
        if (now.isBefore(safe)) {
            return new Decision(Outcome.WAIT_FOR_DEDUPE_TTL, "retry after " + safe);
        }
        return Decision.of(Outcome.REPLAY);
    }

    /** A row of events_skipped with reason undecodable (Flink never stored the id, so no TTL applies). */
    public static Decision decideUndecodable(Facts f) {
        if (f.alreadyReplayed()) {
            return Decision.of(Outcome.ALREADY_REPLAYED);
        }
        Decision common = decideRecord(f);
        return common != null ? common : Decision.of(Outcome.REPLAY);
    }

    private static Decision decideRecord(Facts f) {
        if (f.record() == null) {
            return new Decision(Outcome.UNDECODABLE, f.decodeError());
        }
        if (f.eventIdStored()) {
            return Decision.of(Outcome.EVENT_ID_STORED);
        }
        for (Schema.Field field : f.record().getSchema().getFields()) {
            if (!API_FIELDS.contains(field.name()) && f.record().get(field.pos()) != null) {
                return new Decision(Outcome.NOT_SENDABLE, "field '" + field.name() + "' is not part of the API");
            }
        }
        Object attributes = field(f.record(), "attributes");
        if (attributes instanceof Map<?, ?> map && map.size() >= MAX_ATTRIBUTES) {
            return new Decision(Outcome.NOT_SENDABLE, "already " + map.size() + " attributes; the marker needs one");
        }
        return null;
    }

    /** The API body for one event: the original fields, event_time as timestamp, plus the marker. */
    public static Map<String, Object> eventJson(GenericRecord record, String marker) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("event_id", string(field(record, "event_id")));
        event.put("source", string(field(record, "source")));
        event.put("type", string(field(record, "type")));
        Object time = field(record, "event_time");
        if (time != null) {
            event.put("timestamp", ((Number) time).longValue());
        }
        Object value = field(record, "value");
        if (value != null) {
            event.put("value", ((Number) value).doubleValue());
        }
        Object region = field(record, "region");
        if (region != null) {
            event.put("region", region.toString());
        }
        Map<String, String> attributes = new LinkedHashMap<>();
        if (field(record, "attributes") instanceof Map<?, ?> map) {
            map.forEach((k, v) -> attributes.put(String.valueOf(k), v == null ? null : v.toString()));
        }
        attributes.put(MARKER, marker);
        event.put("attributes", attributes);
        return event;
    }

    private static Object field(GenericRecord record, String name) {
        return record.getSchema().getField(name) == null ? null : record.get(name);
    }

    private static String string(Object value) {
        return value == null ? null : value.toString();
    }
}
