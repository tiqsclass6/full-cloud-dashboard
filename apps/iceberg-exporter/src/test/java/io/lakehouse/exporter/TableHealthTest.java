package io.lakehouse.exporter;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TableHealthTest {

    private static final long MB = 1024L * 1024;
    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

    private InMemoryCatalog catalog;
    private Table table;

    @BeforeEach
    void createTable() {
        catalog = new InMemoryCatalog();
        catalog.initialize("test", Map.of());
        catalog.createNamespace(Namespace.of("lakehouse"));
        table = catalog.createTable(TableIdentifier.of("lakehouse", "events"), SCHEMA, PartitionSpec.unpartitioned());
    }

    @AfterEach
    void closeCatalog() throws Exception {
        catalog.close();
    }

    @Test
    void emptyTableHasNoCommitAndNoCompaction() {
        TableHealth health = TableHealth.inspect(table, 32 * MB);

        assertThat(health.snapshots()).isZero();
        assertThat(health.dataFiles()).isZero();
        assertThat(health.lastCommitMs()).isEqualTo(-1);
        assertThat(health.lastCompactionMs()).isEqualTo(-1);
    }

    @Test
    void streamingAppendsLeaveSmallFiles() {
        // Like the Flink sink: one small file per checkpoint.
        table.newAppend().appendFile(file("a", 2 * MB, 100)).commit();
        table.newAppend().appendFile(file("b", 3 * MB, 150)).commit();
        table.newAppend().appendFile(file("c", 200 * MB, 9000)).commit();

        TableHealth health = TableHealth.inspect(table, 32 * MB);

        assertThat(health.snapshots()).isEqualTo(3);
        assertThat(health.dataFiles()).isEqualTo(3);
        assertThat(health.smallDataFiles()).isEqualTo(2);
        assertThat(health.records()).isEqualTo(9250);
        assertThat(health.sizeBytes()).isEqualTo(205 * MB);
        assertThat(health.manifests()).isEqualTo(3);
        assertThat(health.lastCommitMs()).isEqualTo(table.currentSnapshot().timestampMillis());
        assertThat(health.lastCompactionMs()).isEqualTo(-1);
    }

    @Test
    void compactionReplacesSmallFilesAndIsTimestamped() {
        DataFile a = file("a", 2 * MB, 100);
        DataFile b = file("b", 3 * MB, 150);
        table.newAppend().appendFile(a).appendFile(b).commit();
        table.newRewrite().deleteFile(a).deleteFile(b).addFile(file("ab", 5 * MB + 200 * MB, 250)).commit();

        TableHealth health = TableHealth.inspect(table, 32 * MB);

        assertThat(health.dataFiles()).isEqualTo(1);
        assertThat(health.smallDataFiles()).isZero();
        assertThat(health.records()).isEqualTo(250);
        assertThat(health.lastCompactionMs()).isEqualTo(table.currentSnapshot().timestampMillis());
        assertThat(health.snapshots()).isEqualTo(2);
    }

    @Test
    void aManifestRewriteIsNotACompaction() {
        table.newAppend().appendFile(file("a", 2 * MB, 100)).commit();
        table.newAppend().appendFile(file("b", 3 * MB, 150)).commit();
        // What `--steps compact,manifests` commits when bin-packing found nothing to rewrite.
        table.rewriteManifests().clusterBy(f -> "all").commit();

        assertThat(table.currentSnapshot().operation()).isEqualTo("replace");
        TableHealth health = TableHealth.inspect(table, 32 * MB);

        assertThat(health.manifests()).isEqualTo(1);
        assertThat(health.smallDataFiles()).isEqualTo(2);
        assertThat(health.lastCompactionMs()).isEqualTo(-1);
    }

    private static DataFile file(String name, long sizeBytes, long records) {
        return DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("/warehouse/lakehouse/events/data/" + name + ".parquet")
                .withFileSizeInBytes(sizeBytes)
                .withRecordCount(records)
                .withFormat(FileFormat.PARQUET)
                .build();
    }
}
