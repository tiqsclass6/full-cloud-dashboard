package io.lakehouse.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.junit.jupiter.api.Test;

class ConfluentAvroEncoderTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-28T12:00:00Z"), ZoneOffset.UTC);

    static GenericRecord decode(EventSchema schema, byte[] payload) throws IOException {
        return new GenericDatumReader<GenericRecord>(schema.schema())
                .read(null, DecoderFactory.get().binaryDecoder(payload, ConfluentAvroEncoder.HEADER_SIZE,
                        payload.length - ConfluentAvroEncoder.HEADER_SIZE, null));
    }

    @Test
    void writesConfluentWireFormatHeader() {
        EventSchema schema = new EventSchema(CLOCK);
        byte[] payload = new ConfluentAvroEncoder(schema.schema(), 0x01020304)
                .encode(schema.toRecord(new EventRequest(null, "s", "t", null, null, null), "id-1"));

        assertThat(payload[0]).isEqualTo((byte) 0);
        assertThat(ByteBuffer.wrap(payload, 1, 4).getInt()).isEqualTo(0x01020304);
    }

    @Test
    void roundTripsAllFields() throws IOException {
        EventSchema schema = new EventSchema(CLOCK);
        EventRequest request = new EventRequest(
                null, "host-1", "cpu.usage", CLOCK.millis() - 5000, 0.5, Map.of("region", "us-east-1"));
        byte[] payload = new ConfluentAvroEncoder(schema.schema(), 42)
                .encode(schema.toRecord(request, "11111111-1111-1111-1111-111111111111"));

        GenericRecord decoded = decode(schema, payload);
        assertThat(decoded.get("event_id").toString()).isEqualTo("11111111-1111-1111-1111-111111111111");
        assertThat(decoded.get("source").toString()).isEqualTo("host-1");
        assertThat(decoded.get("type").toString()).isEqualTo("cpu.usage");
        assertThat(decoded.get("event_time")).isEqualTo(CLOCK.millis() - 5000);
        assertThat(decoded.get("ingest_time")).isEqualTo(CLOCK.millis());
        assertThat(decoded.get("value")).isEqualTo(0.5);
        assertThat(decoded.get("attributes")).asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.MAP)
                .hasSize(1);
    }

    @Test
    void carriesTheOptionalRegionAddedInSchemaV2() throws IOException {
        EventSchema schema = new EventSchema(CLOCK);
        byte[] withRegion = new ConfluentAvroEncoder(schema.schema(), 5).encode(
                schema.toRecord(new EventRequest(null, "s", "t", null, null, null, "eu-west-1"), "id"));
        byte[] withoutRegion = new ConfluentAvroEncoder(schema.schema(), 5).encode(
                schema.toRecord(new EventRequest(null, "s", "t", null, null, null), "id"));

        assertThat(decode(schema, withRegion).get("region").toString()).isEqualTo("eu-west-1");
        assertThat(decode(schema, withoutRegion).get("region")).isNull();
    }

    @Test
    void defaultsMissingOptionalFields() throws IOException {
        EventSchema schema = new EventSchema(CLOCK);
        byte[] payload = new ConfluentAvroEncoder(schema.schema(), 1)
                .encode(schema.toRecord(new EventRequest(null, "s", "t", null, null, null), "id"));

        GenericRecord decoded = decode(schema, payload);
        assertThat(decoded.get("value")).isNull();
        assertThat(decoded.get("event_time")).isEqualTo(CLOCK.millis());
        assertThat((Map<?, ?>) decoded.get("attributes")).isEmpty();
    }
}
