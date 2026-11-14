package io.lakehouse.exporter;

import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Settings from environment variables; the catalog variables are the ones the Flink and Spark jobs use. */
public record ExporterConfig(
        String catalogImpl,
        Map<String, String> catalogProperties,
        List<String> tables,
        Duration interval,
        long smallFileBytes,
        int managementPort) {

    static final String REST_CATALOG = "org.apache.iceberg.rest.RESTCatalog";
    static final String GLUE_CATALOG = "org.apache.iceberg.aws.glue.GlueCatalog";
    /** A quarter of the compaction target (128 MiB), the same threshold the Spark maintenance job reports. */
    static final long DEFAULT_SMALL_FILE_BYTES = 32L * 1024 * 1024;

    public static ExporterConfig fromEnv(Map<String, String> env) {
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
        List<String> tables = Arrays.stream(env.getOrDefault("TABLES", "lakehouse.events").split(","))
                .map(t -> t.trim())
                .filter(t -> !t.isEmpty())
                .toList();
        if (tables.isEmpty()) {
            throw new IllegalArgumentException("TABLES must name at least one table (namespace.table)");
        }
        return new ExporterConfig(
                impl,
                Map.copyOf(catalog),
                tables,
                Duration.ofSeconds(Long.parseLong(env.getOrDefault("INTERVAL_SECONDS", "60"))),
                Long.parseLong(env.getOrDefault("SMALL_FILE_BYTES", Long.toString(DEFAULT_SMALL_FILE_BYTES))),
                Integer.parseInt(env.getOrDefault("MANAGEMENT_PORT", "9400")));
    }

    private static void putIfSet(Map<String, String> map, String key, String value) {
        if (value != null && !value.isBlank()) {
            map.put(key, value);
        }
    }
}
