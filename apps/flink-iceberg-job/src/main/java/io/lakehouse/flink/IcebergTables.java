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
