package io.lakehouse.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
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
            schema = IcebergTables.ensureTable(catalog, config).schema();
        } finally {
            IcebergTables.close(catalog);
        }
        LOG.info("Iceberg table {}.{} at schema id {}: {}", config.icebergNamespace(), config.icebergTable(),
                schema.schemaId(), schema.asStruct());

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
                .setValueOnlyDeserializer(new ConfluentAvroRowDeserializer(config.schemaRegistryUrl(), schema))
                .build();

        DataStream<RowData> events = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-events")
                .uid("kafka-source")
                .keyBy(Deduplicator::keyOf)
                .process(new Deduplicator(config.dedupeTtl()))
                .uid("deduplicate")
                .name("deduplicate-by-event-id");

        IcebergSink.forRowData(events)
                .tableLoader(IcebergTables.tableLoader(config))
                .uidSuffix("events-iceberg")
                .append();

        env.execute("events-to-iceberg");
    }
}
