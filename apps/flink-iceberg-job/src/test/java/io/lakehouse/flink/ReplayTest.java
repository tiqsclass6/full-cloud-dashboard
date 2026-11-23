package io.lakehouse.flink;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

class ReplayTest {

    private static final String V2 = """
            {"type":"record","name":"Event","namespace":"io.lakehouse.events","fields":[
              {"name":"event_id","type":"string"},
              {"name":"source","type":"string"},
              {"name":"type","type":"string"},
              {"name":"event_time","type":{"type":"long","logicalType":"timestamp-millis"}},
              {"name":"ingest_time","type":{"type":"long","logicalType":"timestamp-millis"}},
              {"name":"value","type":["null","double"],"default":null},
              {"name":"region","type":["null","string"],"default":null},
              {"name":"attributes","type":{"type":"map","values":"string"},"default":{}}]}""";
    private static final Instant KAFKA_TIME = Instant.parse("2026-11-19T10:00:00Z");
    private static final Duration TTL = Duration.ofMinutes(60);

    private static GenericRecord event(Map<String, String> attributes, String schema) {
        GenericRecord r = new GenericData.Record(new Schema.Parser().parse(schema));
        r.put("event_id", "e-1");
        r.put("source", "host-1");
        r.put("type", "cpu.usage");
        r.put("event_time", 1_700_000_000_000L);
        r.put("ingest_time", 1_700_000_000_500L);
        r.put("value", 0.75);
        r.put("attributes", attributes);
        return r;
    }

    private static Replay.Facts facts(GenericRecord record) {
        return new Replay.Facts(3, 276204L, KAFKA_TIME.toEpochMilli(), false, null, false, null, record, null, false);
    }

    private static Replay.Facts with(Replay.Facts f, boolean inEvents, String skipped, boolean replayed,
            String notInKafka, boolean idStored) {
        return new Replay.Facts(f.partition(), f.offset(), f.kafkaTimestampMs(), inEvents, skipped, replayed,
                notInKafka, f.record(), f.decodeError(), idStored);
    }

    @Test
    void replaysAMissingRecordOnceFlinkHasForgottenItsId() {
        Replay.Facts f = facts(event(Map.of(), V2));
        Instant safe = KAFKA_TIME.plus(TTL).plus(Replay.TTL_MARGIN);

        assertThat(Replay.decideMissing(f, safe.minusSeconds(1), TTL).outcome())
                .isEqualTo(Replay.Outcome.WAIT_FOR_DEDUPE_TTL);
        assertThat(Replay.decideMissing(f, safe.minusSeconds(1), TTL).detail()).contains(safe.toString());
        assertThat(Replay.decideMissing(f, safe, TTL).outcome()).isEqualTo(Replay.Outcome.REPLAY);
    }

    @Test
    void neverReplaysWhatIsAlreadyAccountedFor() {
        Replay.Facts f = facts(event(Map.of(), V2));
        Instant later = KAFKA_TIME.plus(Duration.ofDays(1));

        assertThat(Replay.decideMissing(with(f, true, null, false, null, false), later, TTL).outcome())
                .isEqualTo(Replay.Outcome.STORED);
        assertThat(Replay.decideMissing(with(f, false, "duplicate", false, null, false), later, TTL))
                .isEqualTo(new Replay.Decision(Replay.Outcome.SKIPPED, "duplicate"));
        assertThat(Replay.decideMissing(with(f, false, null, true, null, false), later, TTL).outcome())
                .isEqualTo(Replay.Outcome.ALREADY_REPLAYED);
        assertThat(Replay.decideMissing(with(f, false, null, false, null, true), later, TTL).outcome())
                .isEqualTo(Replay.Outcome.EVENT_ID_STORED);
        assertThat(Replay.decideMissing(with(f, false, null, false, "expired from Kafka", false), later, TTL))
                .isEqualTo(new Replay.Decision(Replay.Outcome.NOT_IN_KAFKA, "expired from Kafka"));
    }

    @Test
    void reportsBytesThatStillCannotBeDecoded() {
        Replay.Facts f = new Replay.Facts(1, 190353L, KAFKA_TIME.toEpochMilli(), false, "undecodable", false, null,
                null, "not a Confluent-format Avro record", false);

        assertThat(Replay.decideUndecodable(f))
                .isEqualTo(new Replay.Decision(Replay.Outcome.UNDECODABLE, "not a Confluent-format Avro record"));
    }

    @Test
    void anUndecodableRecordThatNowDecodesIsReplayedWithoutWaiting() {
        // Flink never stored its id, so its de-duplication state cannot drop the copy.
        Replay.Facts f = new Replay.Facts(1, 190353L, Instant.now().toEpochMilli(), false, "undecodable", false, null,
                event(Map.of(), V2), null, false);

        assertThat(Replay.decideUndecodable(f).outcome()).isEqualTo(Replay.Outcome.REPLAY);
    }

    @Test
    void refusesRecordsTheApiWouldChange() {
        Map<String, String> full = new HashMap<>();
        for (int i = 0; i < Replay.MAX_ATTRIBUTES; i++) {
            full.put("k" + i, "v");
        }
        assertThat(Replay.decideUndecodable(facts(event(full, V2))).outcome()).isEqualTo(Replay.Outcome.NOT_SENDABLE);

        String v3 = V2.replace("{\"name\":\"attributes\"",
                "{\"name\":\"zone\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"attributes\"");
        GenericRecord withZone = event(Map.of(), v3);
        withZone.put("zone", "az1");
        assertThat(Replay.decideUndecodable(facts(withZone)))
                .isEqualTo(new Replay.Decision(Replay.Outcome.NOT_SENDABLE, "field 'zone' is not part of the API"));
    }

    @Test
    void buildsTheApiEventWithTheOriginalIdAndAMarker() {
        GenericRecord r = event(Map.of("host", "a"), V2);
        r.put("region", "eu-west-1");

        Map<String, Object> json = Replay.eventJson(r, Replay.marker(3, 276204L, 1_795_000_000_000L));

        assertThat(json).containsEntry("event_id", "e-1").containsEntry("source", "host-1")
                .containsEntry("type", "cpu.usage").containsEntry("timestamp", 1_700_000_000_000L)
                .containsEntry("value", 0.75).containsEntry("region", "eu-west-1")
                .doesNotContainKeys("ingest_time", "event_time");
        assertThat(json.get("attributes"))
                .isEqualTo(Map.of("host", "a", Replay.MARKER, "3:276204:1795000000000"));
    }

    @Test
    void parsesMarkers() {
        assertThat(Replay.parseMarker("3:276204:1795000000000")).containsExactly(3L, 276204L, 1795000000000L);
        assertThat(Replay.parseMarker("3:x:1")).isNull();
        assertThat(Replay.parseMarker("3:1")).isNull();
        assertThat(Replay.parseMarker(null)).isNull();
    }
}
