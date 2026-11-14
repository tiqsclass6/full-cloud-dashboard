package io.lakehouse.ingest;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/** Settings from environment variables; PRODUCER_LINGER_MS=10 sets the producer's linger.ms=10. */
public record Config(
        String kafkaBootstrapServers,
        String schemaRegistryUrl,
        String topic,
        int httpPort,
        int managementPort,
        int maxInFlight,
        int maxBatchSize,
        int maxBodyBytes,
        Duration publishTimeout,
        Map<String, String> producerOverrides,
        String schemaResource) {

    /** Classpath resource of the Avro schema this instance publishes; override to simulate an older producer. */
    public static final String DEFAULT_SCHEMA_RESOURCE = "/schemas/event.avsc";

    public Config(String kafkaBootstrapServers, String schemaRegistryUrl, String topic, int httpPort,
            int managementPort, int maxInFlight, int maxBatchSize, int maxBodyBytes, Duration publishTimeout,
            Map<String, String> producerOverrides) {
        this(kafkaBootstrapServers, schemaRegistryUrl, topic, httpPort, managementPort, maxInFlight, maxBatchSize,
                maxBodyBytes, publishTimeout, producerOverrides, DEFAULT_SCHEMA_RESOURCE);
    }

    public Config {
        if (maxBatchSize > maxInFlight) {
            throw new IllegalArgumentException("MAX_BATCH_SIZE (" + maxBatchSize
                    + ") must not exceed MAX_IN_FLIGHT (" + maxInFlight + ")");
        }
        producerOverrides = Map.copyOf(producerOverrides);
    }

    public static Config fromEnv(Map<String, String> env) {
        Map<String, String> overrides = new HashMap<>();
        env.forEach((key, value) -> {
            if (key.startsWith("PRODUCER_")) {
                overrides.put(key.substring("PRODUCER_".length()).toLowerCase().replace('_', '.'), value);
            }
        });
        return new Config(
                env.getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                env.getOrDefault("SCHEMA_REGISTRY_URL", "http://localhost:8085/apis/ccompat/v7"),
                env.getOrDefault("TOPIC", "events.raw"),
                Integer.parseInt(env.getOrDefault("HTTP_PORT", "8080")),
                Integer.parseInt(env.getOrDefault("MANAGEMENT_PORT", "8081")),
                Integer.parseInt(env.getOrDefault("MAX_IN_FLIGHT", "10000")),
                Integer.parseInt(env.getOrDefault("MAX_BATCH_SIZE", "500")),
                Integer.parseInt(env.getOrDefault("MAX_BODY_BYTES", "1048576")),
                Duration.ofMillis(Long.parseLong(env.getOrDefault("PUBLISH_TIMEOUT_MS", "10000"))),
                overrides,
                env.getOrDefault("SCHEMA_RESOURCE", DEFAULT_SCHEMA_RESOURCE));
    }

    public String subject() {
        return topic + "-value";
    }
}
