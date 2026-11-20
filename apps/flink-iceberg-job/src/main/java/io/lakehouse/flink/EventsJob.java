package io.lakehouse.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.flink.sink.IcebergSink;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Kafka (Avro) to Iceberg exactly once: offsets live in checkpoints, the sink commits once per checkpoint. */
public final class EventsJob {

    private static final Logger LOG = LoggerFactory.getLogger(EventsJob.class);

    private EventsJob() {}

    // The connector still takes Kafka's deprecated OffsetResetStrategy enum; no replacement overload exists yet.
    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromEnv(System.getenv());

        // The row layout is read at start-up; restart from a savepoint after a schema change.
        Schema schema;
        Catalog catalog = IcebergTables.openCatalog(config);
        try {
            schema = IcebergTables.ensureLineageColumns(IcebergTables.ensureTable(catalog, config)).schema();
            Schema skipped = IcebergTables.ensureSkippedTable(catalog, config).schema();
            if (!IcebergTables.sameColumns(skipped, IcebergTables.skippedSchema())) {
                throw new IllegalStateException(config.icebergSkippedTable() + " does not have the expected schema: "
                        + skipped.asStruct());
            }
        } finally {
            IcebergTables.close(catalog);
        }
        LOG.info("Iceberg table {}.{} at schema id {}: {}", config.icebergNamespace(), config.icebergTable(),
                schema.schemaId(), schema.asStruct());

        RecordLayout layout = RecordLayout.of(schema);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(config.parallelism());
        // The FlinkDeployment configures checkpointing; this is only a fallback.
        if (!env.getCheckpointConfig().isCheckpointingEnabled()) {
            env.enableCheckpointing(10_000);
        }

        KafkaSource<RowData> source = KafkaSource.<RowData>builder()
                .setBootstrapServers(config.kafkaBootstrapServers())
                // Mutual TLS settings (KAFKA_CLIENT_*); empty when Kafka runs without TLS (unit tests).
                .setProperties(config.kafkaClientProperties())
                .setTopics(config.topic())
                .setGroupId(config.consumerGroup())
                // A new job starts from the beginning; a restored job continues from checkpointed offsets.
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new KafkaRecordDecoder(
                        new ConfluentAvroRowDeserializer(config.schemaRegistryUrl(), schema), layout))
                .build();

        // Every Kafka offset ends up in exactly one table: events, or events_skipped (undecodable or duplicate).
        SingleOutputStreamOperator<RowData> decoded = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-events")
                .uid("kafka-source")
                .process(new SkippedRecords.Router(layout), layout.tableTypeInfo())
                .uid("route-undecodable")
                .name("route-undecodable");

        SingleOutputStreamOperator<RowData> events = decoded
                .keyBy(Deduplicator::keyOf)
                .process(new Deduplicator(config.dedupeTtl(), layout), layout.tableTypeInfo())
                .uid("deduplicate")
                .name("deduplicate-by-event-id");

        IcebergSink.forRowData(events)
                .tableLoader(IcebergTables.tableLoader(config))
                .uidSuffix("events-iceberg")
                .append();

        // Its own sink, committed on the same checkpoints as the events table.
        IcebergSink.forRowData(decoded.getSideOutput(SkippedRecords.TAG).union(events.getSideOutput(SkippedRecords.TAG)))
                .tableLoader(IcebergTables.skippedTableLoader(config))
                .uidSuffix("skipped-iceberg")
                .append();

        env.execute("events-to-iceberg");
    }
}
