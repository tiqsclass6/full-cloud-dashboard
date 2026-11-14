package io.lakehouse.exporter;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToDoubleFunction;

/** Metrics for one table; timestamps are epoch seconds (NaN if never), so alerts compute ages with time(). */
final class TableMetrics {

    private final AtomicReference<TableHealth> health = new AtomicReference<>(TableHealth.EMPTY);
    private final AtomicReference<Double> up = new AtomicReference<>(0.0);
    private final Counter successes;
    private final Counter failures;
    private final Timer duration;

    TableMetrics(MeterRegistry registry, String table) {
        // Micrometer's API is null-annotated (JSpecify); checking here proves non-null to the null analysis.
        Objects.requireNonNull(registry, "registry");
        Objects.requireNonNull(table, "table");
        Tags tags = Tags.of("table", table);
        gauge(registry, "iceberg.table.up", "1 if the last inspection succeeded", tags, h -> up.get());
        gauge(registry, "iceberg.table.snapshots", "Snapshots in the table metadata", tags, h -> h.snapshots());
        gauge(registry, "iceberg.table.data.files", "Data files in the current snapshot", tags, h -> h.dataFiles());
        gauge(registry, "iceberg.table.small.data.files", "Data files below the small-file threshold", tags,
                h -> h.smallDataFiles());
        gauge(registry, "iceberg.table.delete.files", "Delete files in the current snapshot", tags, h -> h.deleteFiles());
        gauge(registry, "iceberg.table.records", "Rows in the current snapshot", tags, h -> h.records());
        gauge(registry, "iceberg.table.size.bytes", "Size of the current data files", tags, h -> h.sizeBytes());
        gauge(registry, "iceberg.table.manifests", "Manifest files in the current snapshot", tags, h -> h.manifests());
        gauge(registry, "iceberg.table.last.commit.timestamp.seconds", "Time of the current snapshot", tags,
                h -> seconds(h.lastCommitMs()));
        gauge(registry, "iceberg.table.last.compaction.timestamp.seconds", "Time of the newest compaction", tags,
                h -> seconds(h.lastCompactionMs()));
        successes = Counter.builder("iceberg.exporter.inspections").tags(tags).tag("result", "success")
                .description("Table inspections").register(registry);
        failures = Counter.builder("iceberg.exporter.inspections").tags(tags).tag("result", "failure")
                .description("Table inspections").register(registry);
        duration = Timer.builder("iceberg.exporter.inspection.duration").tags(tags)
                .description("Time to inspect the table").register(registry);
    }

    void record(TableHealth latest, long nanos) {
        health.set(latest);
        up.set(1.0);
        successes.increment();
        duration.record(nanos, java.util.concurrent.TimeUnit.NANOSECONDS);
    }

    /** Keeps the last good values (they are still the best knowledge) but marks the table down. */
    void failed() {
        up.set(0.0);
        failures.increment();
    }

    private void gauge(MeterRegistry registry, String name, String help, Tags tags, ToDoubleFunction<TableHealth> value) {
        Objects.requireNonNull(registry, "registry");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(tags, "tags");
        Gauge.builder(name, health, ref -> value.applyAsDouble(ref.get())).tags(tags).description(help).register(registry);
    }

    private static double seconds(long epochMillis) {
        return epochMillis < 0 ? Double.NaN : epochMillis / 1000.0;
    }
}
