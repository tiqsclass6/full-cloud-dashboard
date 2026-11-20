package io.lakehouse.flink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConfluentAvroRowDeserializerTest {

    private static final String V1 = """
            {"type":"record","name":"Event","namespace":"io.lakehouse.events","fields":[
              {"name":"event_id","type":"string"},
              {"name":"source","type":"string"},
              {"name":"type","type":"string"},
              {"name":"event_time","type":{"type":"long","logicalType":"timestamp-millis"}},
              {"name":"ingest_time","type":{"type":"long","logicalType":"timestamp-millis"}},
              {"name":"value","type":["null","double"],"default":null},
              {"name":"attributes","type":{"type":"map","values":"string"},"default":{}}]}""";

    /** v2 adds an optional "region" field. */
    private static final String V2 = V1.replace(
            "{\"name\":\"attributes\"", "{\"name\":\"region\",\"type\":[\"null\",\"string\"],\"default\":null},"
                    + "{\"name\":\"attributes\"");

    private HttpServer registry;
    private String registryUrl;

    @BeforeEach
    void startRegistry() throws IOException {
        registry = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        serve(1, V1);
        serve(2, V2);
        registry.start();
        registryUrl = "http://127.0.0.1:" + registry.getAddress().getPort();
    }

    @AfterEach
    void stopRegistry() {
        registry.stop(0);
    }

    private void serve(int id, String schema) {
        registry.createContext("/schemas/ids/" + id, exchange -> {
            byte[] body = ("{\"schema\":" + new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(schema) + "}")
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
    }

    private static byte[] encode(int schemaId, String schemaJson, Map<String, Object> fields) throws IOException {
        Schema schema = new Schema.Parser().parse(schemaJson);
        GenericRecord record = new GenericData.Record(schema);
        fields.forEach(record::put);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(0);
        out.write(new byte[] {0, 0, 0, (byte) schemaId});
        var encoder = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(schema).write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    private static Map<String, Object> baseFields() {
        Map<String, Object> fields = new HashMap<>();
        fields.put("event_id", "e-1");
        fields.put("source", "host-1");
        fields.put("type", "cpu.usage");
        fields.put("event_time", 1_700_000_000_000L);
        fields.put("ingest_time", 1_700_000_000_500L);
        fields.put("value", 0.75);
        fields.put("attributes", Map.of("region", "us-east-1"));
        return fields;
    }

    @Test
    void decodesAV1RecordIntoTheTableLayout() throws Exception {
        var deserializer = new ConfluentAvroRowDeserializer(registryUrl, IcebergTables.initialSchema());

        RowData row = deserializer.deserialize(encode(1, V1, baseFields()));

        assertThat(row.getArity()).isEqualTo(7);
        assertThat(row.getString(0).toString()).isEqualTo("e-1");
        assertThat(row.getString(1).toString()).isEqualTo("host-1");
        assertThat(row.getTimestamp(3, 6).getMillisecond()).isEqualTo(1_700_000_000_000L);
        assertThat(row.getDouble(5)).isEqualTo(0.75);
        GenericMapData attributes = (GenericMapData) row.getMap(6);
        assertThat(attributes.size()).isEqualTo(1);
    }

    @Test
    void ignoresFieldsTheTableDoesNotHaveYet() throws Exception {
        // Producer already on v2, table still on v1: the new field is dropped, nothing breaks.
        var deserializer = new ConfluentAvroRowDeserializer(registryUrl, IcebergTables.initialSchema());
        Map<String, Object> fields = baseFields();
        fields.put("region", "eu-west-1");

        RowData row = deserializer.deserialize(encode(2, V2, fields));

        assertThat(row.getArity()).isEqualTo(7);
        assertThat(row.getString(0).toString()).isEqualTo("e-1");
    }

    @Test
    void fillsNewTableColumnsWithNullForOldRecords() throws Exception {
        // Table already has the new column, but this record was written with v1: region is null.
        org.apache.iceberg.Schema evolved = new org.apache.iceberg.Schema(
                IcebergTables.initialSchema().columns().get(0),
                IcebergTables.initialSchema().columns().get(1),
                IcebergTables.initialSchema().columns().get(2),
                IcebergTables.initialSchema().columns().get(3),
                IcebergTables.initialSchema().columns().get(4),
                IcebergTables.initialSchema().columns().get(5),
                IcebergTables.initialSchema().columns().get(6),
                Types.NestedField.optional(10, "region", Types.StringType.get()));
        var deserializer = new ConfluentAvroRowDeserializer(registryUrl, evolved);

        RowData oldRecord = deserializer.deserialize(encode(1, V1, baseFields()));
        Map<String, Object> newFields = baseFields();
        newFields.put("region", "eu-west-1");
        RowData newRecord = deserializer.deserialize(encode(2, V2, newFields));

        assertThat(oldRecord.getArity()).isEqualTo(8);
        assertThat(oldRecord.isNullAt(7)).isTrue();
        assertThat(newRecord.getString(7).toString()).isEqualTo("eu-west-1");
    }

    @Test
    void reportsPoisonRecordsAsPoison() throws Exception {
        var deserializer = new ConfluentAvroRowDeserializer(registryUrl, IcebergTables.initialSchema());

        assertThatThrownBy(() -> deserializer.deserialize("not avro".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(ConfluentAvroRowDeserializer.PoisonRecordException.class);
        // Unknown schema id: the registry answers 404.
        assertThatThrownBy(() -> deserializer.deserialize(new byte[] {0, 0, 0, 0, 99, 1, 2, 3}))
                .isInstanceOf(ConfluentAvroRowDeserializer.PoisonRecordException.class)
                .hasMessageContaining("404");
        assertThatThrownBy(() -> deserializer.deserialize(null))
                .isInstanceOf(ConfluentAvroRowDeserializer.PoisonRecordException.class);
        // A known schema id followed by truncated Avro.
        assertThatThrownBy(() -> deserializer.deserialize(new byte[] {0, 0, 0, 0, 1, 2}))
                .isInstanceOf(ConfluentAvroRowDeserializer.PoisonRecordException.class);
    }

    @Test
    void failsInsteadOfSkippingWhenTheRegistryIsDown() throws Exception {
        int[] calls = {0};
        registry.createContext("/schemas/ids/4", exchange -> {
            calls[0]++;
            exchange.sendResponseHeaders(503, -1);
            exchange.close();
        });
        var deserializer = new ConfluentAvroRowDeserializer(registryUrl, IcebergTables.initialSchema());
        byte[] good = encode(4, V1, baseFields());

        assertThatThrownBy(() -> deserializer.deserialize(good))
                .isInstanceOf(ConfluentAvroRowDeserializer.RegistryUnavailableException.class)
                .hasMessageContaining("503");
        assertThat(calls[0]).isEqualTo(1);

        // Nothing listens on port 1: connection refused.
        var unreachable = new ConfluentAvroRowDeserializer("http://127.0.0.1:1", IcebergTables.initialSchema());
        assertThatThrownBy(() -> unreachable.deserialize(good))
                .isInstanceOf(ConfluentAvroRowDeserializer.RegistryUnavailableException.class)
                .hasMessageContaining("unreachable");
    }

    @Test
    void rejectsRecordsMissingRequiredColumns() throws Exception {
        var deserializer = new ConfluentAvroRowDeserializer(registryUrl, IcebergTables.initialSchema());
        Map<String, Object> fields = baseFields();
        fields.put("source", null);
        // Avro itself refuses to encode a null non-nullable field, so craft a v2-style schema with nullable source.
        String nullableSource = V1.replace("{\"name\":\"source\",\"type\":\"string\"}",
                "{\"name\":\"source\",\"type\":[\"null\",\"string\"],\"default\":null}");
        registry.createContext("/schemas/ids/3", exchange -> {
            byte[] body = ("{\"schema\":" + new com.fasterxml.jackson.databind.ObjectMapper()
                    .writeValueAsString(nullableSource) + "}").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });

        assertThatThrownBy(() -> deserializer.deserialize(encode(3, nullableSource, fields)))
                .isInstanceOf(ConfluentAvroRowDeserializer.PoisonRecordException.class)
                .hasMessageContaining("required column 'source'");
    }

    private static RecordLayout lineageLayout() {
        return RecordLayout.of(IcebergTables.withLineage(IcebergTables.initialSchema()));
    }

    private static org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> kafka(int partition, long offset,
            byte[] value) {
        return new org.apache.kafka.clients.consumer.ConsumerRecord<>("events.raw", partition, offset,
                1_700_000_001_000L, org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, value.length,
                "host-1".getBytes(StandardCharsets.UTF_8), value, new org.apache.kafka.common.header.internals.RecordHeaders(),
                java.util.Optional.empty());
    }

    @Test
    void addsTheKafkaPartitionOffsetAndTimestampToEachRow() throws Exception {
        RecordLayout layout = lineageLayout();
        var decoder = new KafkaRecordDecoder(new ConfluentAvroRowDeserializer(registryUrl,
                IcebergTables.withLineage(IcebergTables.initialSchema())), layout);

        RowData row = decoder.decode(kafka(4, 1234L, encode(1, V1, baseFields())));

        assertThat(row.getArity()).isEqualTo(layout.arity());
        assertThat(row.getString(0).toString()).isEqualTo("e-1");
        assertThat(row.getInt(layout.partition())).isEqualTo(4);
        assertThat(row.getLong(layout.offset())).isEqualTo(1234L);
        assertThat(row.getTimestamp(layout.timestamp(), 6).getMillisecond()).isEqualTo(1_700_000_001_000L);
        assertThat(row.isNullAt(layout.error())).isTrue();
        assertThat(row.isNullAt(layout.payload())).isTrue();
    }

    @Test
    void keepsAnUndecodableRecordWithItsErrorAndBytes() throws Exception {
        RecordLayout layout = lineageLayout();
        var decoder = new KafkaRecordDecoder(new ConfluentAvroRowDeserializer(registryUrl,
                IcebergTables.withLineage(IcebergTables.initialSchema())), layout);
        byte[] poison = "not avro".getBytes(StandardCharsets.UTF_8);

        RowData row = decoder.decode(kafka(2, 77L, poison));

        assertThat(row.isNullAt(0)).isTrue();
        assertThat(row.getInt(layout.partition())).isEqualTo(2);
        assertThat(row.getLong(layout.offset())).isEqualTo(77L);
        assertThat(row.getString(layout.error()).toString()).contains("not a Confluent-format Avro record");
        assertThat(row.getBinary(layout.payload())).isEqualTo(poison);

        RowData skipped = SkippedRecords.of(row, layout, SkippedRecords.UNDECODABLE);
        assertThat(skipped.getInt(0)).isEqualTo(2);
        assertThat(skipped.getLong(1)).isEqualTo(77L);
        assertThat(skipped.getString(3).toString()).isEqualTo("undecodable");
        assertThat(skipped.isNullAt(4)).isTrue();
        assertThat(skipped.getBinary(7)).isEqualTo(poison);
    }

    @Test
    void failsTheReadWhenTheRegistryIsDownSoTheRecordIsReadAgain() throws Exception {
        RecordLayout layout = lineageLayout();
        var decoder = new KafkaRecordDecoder(new ConfluentAvroRowDeserializer("http://127.0.0.1:1",
                IcebergTables.withLineage(IcebergTables.initialSchema())), layout);

        assertThatThrownBy(() -> decoder.decode(kafka(0, 1L, encode(1, V1, baseFields()))))
                .isInstanceOf(ConfluentAvroRowDeserializer.RegistryUnavailableException.class);
    }
}
