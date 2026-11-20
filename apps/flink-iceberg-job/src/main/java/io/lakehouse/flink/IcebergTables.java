package io.lakehouse.flink;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.types.Types;

/** Creates the events table on first run and builds the loaders the sink needs. */
public final class IcebergTables {

    private IcebergTables() {}

    /** The v1 schema of the events table, matching event.avsc as first released. */
    public static Schema initialSchema() {
        return new Schema(
                Types.NestedField.required(1, "event_id", Types.StringType.get()),
                Types.NestedField.required(2, "source", Types.StringType.get()),
                Types.NestedField.required(3, "type", Types.StringType.get()),
                Types.NestedField.required(4, "event_time", Types.TimestampType.withZone()),
                Types.NestedField.required(5, "ingest_time", Types.TimestampType.withZone()),
                Types.NestedField.optional(6, "value", Types.DoubleType.get()),
                Types.NestedField.required(7, "attributes",
                        Types.MapType.ofRequired(8, 9, Types.StringType.get(), Types.StringType.get())));
    }

    /** Where each row came from in Kafka; added to the events table at start-up (null in rows written before). */
    public static final String KAFKA_PARTITION = "kafka_partition";
    public static final String KAFKA_OFFSET = "kafka_offset";
    public static final String KAFKA_TIMESTAMP = "kafka_timestamp";

    /** Every Kafka record the job read but did not store in the events table, so each offset is accounted for. */
    public static Schema skippedSchema() {
        return new Schema(
                Types.NestedField.required(1, KAFKA_PARTITION, Types.IntegerType.get()),
                Types.NestedField.required(2, KAFKA_OFFSET, Types.LongType.get()),
                Types.NestedField.required(3, KAFKA_TIMESTAMP, Types.TimestampType.withZone()),
                Types.NestedField.required(4, "reason", Types.StringType.get()),
                Types.NestedField.optional(5, "event_id", Types.StringType.get()),
                Types.NestedField.optional(6, "source", Types.StringType.get()),
                Types.NestedField.optional(7, "error", Types.StringType.get()),
                // The record value as read from Kafka, kept only for undecodable records so they can be reprocessed.
                Types.NestedField.optional(8, "payload", Types.BinaryType.get()),
                Types.NestedField.required(9, "recorded_at", Types.TimestampType.withZone()));
    }

    /** {@code schema} with the lineage columns appended, as {@link #ensureLineageColumns} leaves a table. */
    public static Schema withLineage(Schema schema) {
        java.util.List<Types.NestedField> columns = new java.util.ArrayList<>(schema.columns());
        int id = schema.highestFieldId();
        columns.add(Types.NestedField.optional(++id, KAFKA_PARTITION, Types.IntegerType.get()));
        columns.add(Types.NestedField.optional(++id, KAFKA_OFFSET, Types.LongType.get()));
        columns.add(Types.NestedField.optional(++id, KAFKA_TIMESTAMP, Types.TimestampType.withZone()));
        return new Schema(columns);
    }

    /** Adds the Kafka lineage columns if the table predates them (metadata only; old rows read back null). */
    public static Table ensureLineageColumns(Table table) {
        org.apache.iceberg.UpdateSchema update = table.updateSchema();
        boolean changed = false;
        if (table.schema().findField(KAFKA_PARTITION) == null) {
            update.addColumn(KAFKA_PARTITION, Types.IntegerType.get());
            changed = true;
        }
        if (table.schema().findField(KAFKA_OFFSET) == null) {
            update.addColumn(KAFKA_OFFSET, Types.LongType.get());
            changed = true;
        }
        if (table.schema().findField(KAFKA_TIMESTAMP) == null) {
            update.addColumn(KAFKA_TIMESTAMP, Types.TimestampType.withZone());
            changed = true;
        }
        if (changed) {
            update.commit();
            table.refresh();
        }
        return table;
    }

    /** Creates the skipped-records table if missing (partitioned by day of the Kafka timestamp). */
    public static Table ensureSkippedTable(Catalog catalog, JobConfig config) {
        TableIdentifier id = TableIdentifier.of(config.icebergNamespace(), config.icebergSkippedTable());
        if (catalog.tableExists(id)) {
            return catalog.loadTable(id);
        }
        Schema schema = skippedSchema();
        try {
            return catalog.buildTable(id, schema)
                    .withPartitionSpec(PartitionSpec.builderFor(schema).day(KAFKA_TIMESTAMP).build())
                    .withProperties(Map.of(
                            "format-version", "2",
                            "write.format.default", "parquet",
                            "write.parquet.compression-codec", "zstd",
                            "commit.retry.num-retries", "10"))
                    .create();
        } catch (AlreadyExistsException e) {
            return catalog.loadTable(id);
        }
    }

    /** Same column names, types and order; field ids may differ (a catalog can reassign them on create). */
    public static boolean sameColumns(Schema a, Schema b) {
        if (a.columns().size() != b.columns().size()) {
            return false;
        }
        for (int i = 0; i < a.columns().size(); i++) {
            Types.NestedField x = a.columns().get(i);
            Types.NestedField y = b.columns().get(i);
            if (!x.name().equals(y.name()) || !x.type().equals(y.type()) || x.isOptional() != y.isOptional()) {
                return false;
            }
        }
        return true;
    }

    public static TableLoader skippedTableLoader(JobConfig config) {
        return TableLoader.fromCatalog(catalogLoader(config),
                TableIdentifier.of(config.icebergNamespace(), config.icebergSkippedTable()));
    }

    public static CatalogLoader catalogLoader(JobConfig config) {
        return CatalogLoader.custom("lakehouse", config.catalogProperties(), new Configuration(), config.catalogImpl());
    }

    public static TableLoader tableLoader(JobConfig config) {
        return TableLoader.fromCatalog(catalogLoader(config),
                TableIdentifier.of(config.icebergNamespace(), config.icebergTable()));
    }

    /** Opens a catalog client. The caller owns it and must close it with {@link #close(Catalog)}. */
    public static Catalog openCatalog(JobConfig config) {
        return catalogLoader(config).loadCatalog();
    }

    /** Closes a catalog client; the REST catalog holds an HTTP connection pool. */
    public static void close(Catalog catalog) {
        if (catalog instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception ignored) {
                // best effort
            }
        }
    }

    /** Creates the namespace and table if missing; the table stays usable only while {@code catalog} is open. */
    public static Table ensureTable(Catalog catalog, JobConfig config) {
        Namespace namespace = Namespace.of(config.icebergNamespace());
        if (catalog instanceof SupportsNamespaces namespaces && !namespaces.namespaceExists(namespace)) {
            try {
                namespaces.createNamespace(namespace);
            } catch (AlreadyExistsException ignored) {
                // created concurrently by another run
            }
        }
        TableIdentifier id = TableIdentifier.of(namespace, config.icebergTable());
        if (catalog.tableExists(id)) {
            return catalog.loadTable(id);
        }
        Schema schema = initialSchema();
        PartitionSpec spec = PartitionSpec.builderFor(schema).day("event_time").build();
        try {
            return catalog.buildTable(id, schema)
                    .withPartitionSpec(spec)
                    .withProperties(Map.of(
                            "format-version", "2",
                            "write.format.default", "parquet",
                            "write.parquet.compression-codec", "zstd",
                            "commit.retry.num-retries", "10"))
                    .create();
        } catch (AlreadyExistsException e) {
            return catalog.loadTable(id);
        }
    }
}
