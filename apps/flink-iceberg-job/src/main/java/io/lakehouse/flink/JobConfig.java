package io.lakehouse.flink;

import java.io.Serializable;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/** Job settings, read from environment variables (set on the FlinkDeployment pod template). */
public record JobConfig(
        String kafkaBootstrapServers,
        String topic,
        String consumerGroup,
        String schemaRegistryUrl,
        String icebergNamespace,
        String icebergTable,
        String icebergSkippedTable,
        String catalogImpl,
        Map<String, String> catalogProperties,
        Duration dedupeTtl,
        int parallelism,
        Map<String, String> kafkaProperties) implements Serializable {

    static final String REST_CATALOG = "org.apache.iceberg.rest.RESTCatalog";
    static final String GLUE_CATALOG = "org.apache.iceberg.aws.glue.GlueCatalog";
    /** KAFKA_CLIENT_SECURITY_PROTOCOL=SSL sets security.protocol (prefix stripped, _ to .); used for mutual TLS. */
    static final String KAFKA_CLIENT_PREFIX = "KAFKA_CLIENT_";

    /** ICEBERG_CATALOG_TYPE: "rest" (default, local) or "glue" (AWS). */
    public static JobConfig fromEnv(Map<String, String> env) {
        String type = env.getOrDefault("ICEBERG_CATALOG_TYPE", "rest");
        Map<String, String> catalog = new LinkedHashMap<>();
        String impl;
        switch (type) {
            case "rest" -> {
                impl = REST_CATALOG;
                catalog.put("uri", env.getOrDefault("ICEBERG_REST_URI", "http://iceberg-rest.data.svc:8181"));
            }
            case "glue" -> impl = GLUE_CATALOG;
            default -> throw new IllegalArgumentException("ICEBERG_CATALOG_TYPE must be rest or glue, got " + type);
        }
        catalog.put("warehouse", env.getOrDefault("ICEBERG_WAREHOUSE", "s3://warehouse/"));
        catalog.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        catalog.put("client.region", env.getOrDefault("AWS_REGION", "us-east-1"));
        // Local SeaweedFS needs an endpoint, path-style access and static keys; on AWS IRSA applies.
        putIfSet(catalog, "s3.endpoint", env.get("S3_ENDPOINT"));
        putIfSet(catalog, "s3.access-key-id", env.get("S3_ACCESS_KEY_ID"));
        putIfSet(catalog, "s3.secret-access-key", env.get("S3_SECRET_ACCESS_KEY"));
        if (env.get("S3_ENDPOINT") != null) {
            catalog.put("s3.path-style-access", "true");
        }
        Map<String, String> kafka = new LinkedHashMap<>();
        env.forEach((key, value) -> {
            if (key.startsWith(KAFKA_CLIENT_PREFIX)) {
                kafka.put(key.substring(KAFKA_CLIENT_PREFIX.length()).toLowerCase().replace('_', '.'), value);
            }
        });
        return new JobConfig(
                env.getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "lakehouse-kafka-bootstrap.kafka.svc:9093"),
                env.getOrDefault("TOPIC", "events.raw"),
                env.getOrDefault("CONSUMER_GROUP", "flink-events-to-iceberg"),
                env.getOrDefault("SCHEMA_REGISTRY_URL", "http://apicurio-registry.data.svc:8080/apis/ccompat/v7"),
                env.getOrDefault("ICEBERG_NAMESPACE", "lakehouse"),
                env.getOrDefault("ICEBERG_TABLE", "events"),
                // Every Kafka record the job does not store in the events table: duplicate copies and undecodable records.
                env.getOrDefault("ICEBERG_SKIPPED_TABLE", "events_skipped"),
                impl,
                Map.copyOf(catalog),
                Duration.ofMinutes(Long.parseLong(env.getOrDefault("DEDUPE_TTL_MINUTES", "60"))),
                Integer.parseInt(env.getOrDefault("PARALLELISM", "2")),
                Map.copyOf(kafka));
    }

    public Properties kafkaClientProperties() {
        Properties properties = new Properties();
        properties.putAll(kafkaProperties);
        return properties;
    }

    private static void putIfSet(Map<String, String> map, String key, String value) {
        if (value != null && !value.isBlank()) {
            map.put(key, value);
        }
    }
}
