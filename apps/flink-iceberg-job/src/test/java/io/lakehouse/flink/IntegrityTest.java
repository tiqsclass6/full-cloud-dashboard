package io.lakehouse.flink;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.Test;

/** Every Kafka offset leaves the job exactly once: as an events row or as an events_skipped row. */
class IntegrityTest {

    private static final Schema TABLE = IcebergTables.withLineage(IcebergTables.initialSchema());
    private static final RecordLayout LAYOUT = RecordLayout.of(TABLE);

    private static RowData decoded(String eventId, long offset) {
        GenericRowData row = new GenericRowData(LAYOUT.arity());
        row.setField(0, StringData.fromString(eventId));
        row.setField(1, StringData.fromString("host-1"));
        row.setField(2, StringData.fromString("cpu.usage"));
        row.setField(3, TimestampData.fromEpochMillis(1_700_000_000_000L));
        row.setField(4, TimestampData.fromEpochMillis(1_700_000_000_500L));
        row.setField(6, new GenericMapData(Map.of()));
        lineage(row, offset);
        return row;
    }

    private static RowData undecodable(long offset) {
        GenericRowData row = new GenericRowData(LAYOUT.arity());
        row.setField(LAYOUT.error(), StringData.fromString("not a Confluent-format Avro record"));
        row.setField(LAYOUT.payload(), "junk".getBytes(StandardCharsets.UTF_8));
        lineage(row, offset);
        return row;
    }

    private static void lineage(GenericRowData row, long offset) {
        row.setField(LAYOUT.partition(), 0);
        row.setField(LAYOUT.offset(), offset);
        row.setField(LAYOUT.timestamp(), TimestampData.fromEpochMillis(1_700_000_001_000L));
    }

    @Test
    void routesEachOffsetToExactlyOneTable() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<RowData> records = env.fromData(List.of(decoded("a", 0), decoded("b", 1), undecodable(2),
                decoded("a", 3), decoded("c", 4), decoded("b", 5)), LAYOUT.recordTypeInfo());
        SingleOutputStreamOperator<RowData> routed = records
                .process(new SkippedRecords.Router(LAYOUT), LAYOUT.tableTypeInfo())
                // As in the job: one partition's records pass through one source subtask and its chained router.
                .setParallelism(1);
        SingleOutputStreamOperator<RowData> events = routed
                .keyBy(Deduplicator::keyOf)
                .process(new Deduplicator(Duration.ofHours(1), LAYOUT), LAYOUT.tableTypeInfo());

        DataStream<String> stored = events.map(r -> "events:" + r.getString(0) + "@" + r.getLong(LAYOUT.offset()))
                .returns(Types.STRING);
        DataStream<String> skipped = routed.getSideOutput(SkippedRecords.TAG)
                .union(events.getSideOutput(SkippedRecords.TAG))
                .map(r -> "skipped:" + r.getString(3) + ":" + (r.isNullAt(4) ? "-" : r.getString(4)) + "@" + r.getLong(1))
                .returns(Types.STRING);

        List<String> out = new ArrayList<>();
        CloseableIterator<String> it = stored.union(skipped).executeAndCollect();
        try {
            it.forEachRemaining(out::add);
        } finally {
            it.close();
        }

        // Within one partition the first copy of an id wins (keyed state); offsets 0-5 each appear once.
        assertThat(out).containsExactlyInAnyOrder("events:a@0", "events:b@1", "skipped:undecodable:-@2",
                "skipped:duplicate:a@3", "events:c@4", "skipped:duplicate:b@5");
        assertThat(LAYOUT.tableType().getFieldCount()).isEqualTo(TABLE.columns().size());
    }

    @Test
    void addsLineageColumnsOnceAndCreatesTheSkippedTable() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Map.of());
        JobConfig config = JobConfig.fromEnv(Map.of());

        Table events = IcebergTables.ensureLineageColumns(IcebergTables.ensureTable(catalog, config));
        int schemaId = events.schema().schemaId();
        Table again = IcebergTables.ensureLineageColumns(IcebergTables.ensureTable(catalog, config));

        assertThat(again.schema().schemaId()).isEqualTo(schemaId); // no second schema change
        assertThat(IcebergTables.sameColumns(again.schema(), TABLE)).isTrue();
        Table skipped = IcebergTables.ensureSkippedTable(catalog, config);
        assertThat(IcebergTables.sameColumns(skipped.schema(), IcebergTables.skippedSchema())).isTrue();
        assertThat(skipped.spec().fields()).hasSize(1);
    }
}
