package io.lakehouse.exporter;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;

/** One table at its current snapshot. Timestamps are epoch ms, or -1 when the event has not happened yet. */
public record TableHealth(
        int snapshots,
        long dataFiles,
        long smallDataFiles,
        long deleteFiles,
        long records,
        long sizeBytes,
        int manifests,
        long lastCommitMs,
        long lastCompactionMs) {

    static final TableHealth EMPTY = new TableHealth(0, 0, 0, 0, 0, 0, 0, -1, -1);

    /** Reads the table's metadata and manifests (no data files) and computes its health. */
    public static TableHealth inspect(Table table, long smallFileBytes) {
        Snapshot current = table.currentSnapshot();
        int snapshots = 0;
        long lastCompaction = -1;
        for (Snapshot snapshot : table.snapshots()) {
            snapshots++;
            if (isCompaction(snapshot)) {
                lastCompaction = Math.max(lastCompaction, snapshot.timestampMillis());
            }
        }
        if (current == null) {
            return new TableHealth(snapshots, 0, 0, 0, 0, 0, 0, -1, lastCompaction);
        }

        long dataFiles = 0;
        long small = 0;
        long deletes = 0;
        long records = 0;
        long bytes = 0;
        try (CloseableIterable<FileScanTask> tasks = table.newScan().useSnapshot(current.snapshotId()).planFiles()) {
            for (FileScanTask task : tasks) {
                dataFiles++;
                long size = task.file().fileSizeInBytes();
                bytes += size;
                records += task.file().recordCount();
                if (size < smallFileBytes) {
                    small++;
                }
                deletes += task.deletes().size();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("could not plan files for " + table.name(), e);
        }
        int manifests = current.allManifests(table.io()).size();
        return new TableHealth(snapshots, dataFiles, small, deletes, records, bytes, manifests,
                current.timestampMillis(), lastCompaction);
    }

    /** A {@code replace} snapshot that removed data files (rewrite_manifests also commits {@code replace}). */
    static boolean isCompaction(Snapshot snapshot) {
        if (!DataOperations.REPLACE.equals(snapshot.operation()) || snapshot.summary() == null) {
            return false;
        }
        String deleted = snapshot.summary().get(SnapshotSummary.DELETED_FILES_PROP);
        try {
            return deleted != null && Long.parseLong(deleted) > 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
