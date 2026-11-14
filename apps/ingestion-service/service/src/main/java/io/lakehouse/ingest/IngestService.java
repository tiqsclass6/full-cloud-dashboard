package io.lakehouse.ingest;

import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The running service: wires the producer, schema, metrics and the two HTTP servers. */
public final class IngestService implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(IngestService.class);

    private final Config config;
    private final AtomicBoolean ready = new AtomicBoolean(false);
    private ManagementServer management;
    private IngestServer server;
    private Producer<String, byte[]> producer;
    private KafkaClientMetrics kafkaMetrics;
    private JvmGcMetrics gcMetrics;

    public IngestService(Config config) {
        this.config = config;
    }

    /** Starts everything and returns once the service is ready to accept events. */
    public void start() throws IOException, InterruptedException {
        LOG.info("Starting ingestion-service: topic={} bootstrap={} registry={} maxInFlight={}",
                config.topic(), config.kafkaBootstrapServers(), config.schemaRegistryUrl(), config.maxInFlight());

        Clock clock = Clock.systemUTC();
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        registry.config().commonTags("application", "ingestion-service");
        bindJvmMetrics(registry);

        Semaphore permits = new Semaphore(config.maxInFlight());
        IngestMetrics metrics = new IngestMetrics(registry, permits, config.maxInFlight());

        // Liveness and metrics are up immediately; readiness flips once Kafka and the registry are reachable.
        management = new ManagementServer(config.managementPort(), registry, ready::get);
        management.start();

        // A local, not the field: the IDE's null analysis can prove a fresh object non-null only through a local.
        KafkaProducer<String, byte[]> kafkaProducer = new KafkaProducer<>(producerProperties(config));
        producer = kafkaProducer;
        kafkaMetrics = new KafkaClientMetrics(kafkaProducer);
        kafkaMetrics.bindTo(registry);

        EventSchema schema = new EventSchema(clock, config.schemaResource());
        SchemaRegistryClient registryClient = new SchemaRegistryClient(config.schemaRegistryUrl());
        int schemaId = registryClient.registerWithRetry(config.subject(), schema.json(), 60, Duration.ofSeconds(2));
        LOG.info("Schema registered: subject={} id={}", config.subject(), schemaId);
        awaitTopic(producer, config.topic());

        EventIngestor ingestor = new EventIngestor(producer, new ConfluentAvroEncoder(schema.schema(), schemaId),
                schema, new EventValidator(clock), metrics, permits, config);
        server = new IngestServer(config.httpPort(), new IngestHandler(ingestor, metrics, config.maxBodyBytes()));
        server.start();
        ready.set(true);
        LOG.info("Ready: ingest on :{}, management on :{}", server.port(), management.port());
    }

    public int ingestPort() {
        return server.port();
    }

    public int managementPort() {
        return management.port();
    }

    @Override
    public void close() {
        LOG.info("Shutting down");
        ready.set(false);
        if (server != null) {
            server.stop();
        }
        if (producer != null) {
            producer.close(Duration.ofSeconds(10));
        }
        if (kafkaMetrics != null) {
            kafkaMetrics.close();
        }
        if (gcMetrics != null) {
            gcMetrics.close();
        }
        if (management != null) {
            management.stop();
        }
    }

    private static Properties producerProperties(Config config) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "ingestion-service");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        // Durability first: all in-sync replicas acknowledge, and idempotence stops retries duplicating or reordering.
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        // Throughput: a small linger and larger batches, compressed.
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "65536");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
        props.putAll(config.producerOverrides());
        Properties properties = new Properties();
        properties.putAll(props);
        return properties;
    }

    private static void awaitTopic(Producer<String, byte[]> producer, String topic) throws InterruptedException {
        for (int attempt = 1; ; attempt++) {
            try {
                LOG.info("Topic {} has {} partitions", topic, producer.partitionsFor(topic).size());
                return;
            } catch (RuntimeException e) {
                if (attempt >= 60) {
                    throw e;
                }
                LOG.warn("Topic metadata not available yet (attempt {}): {}", attempt, e.getMessage());
                Thread.sleep(2000);
            }
        }
    }

    private void bindJvmMetrics(PrometheusMeterRegistry registry) {
        new JvmMemoryMetrics().bindTo(java.util.Objects.requireNonNull(registry));
        gcMetrics = new JvmGcMetrics();
        gcMetrics.bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);
        new ClassLoaderMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);
        new UptimeMetrics().bindTo(registry);
        new FileDescriptorMetrics().bindTo(registry);
    }
}
