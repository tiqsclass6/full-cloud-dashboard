package io.lakehouse.exporter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TableMetricsTest {

    @Test
    void exportsHealthAsPrometheusMetrics() {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        TableMetrics metrics = new TableMetrics(registry, "lakehouse.events");

        metrics.record(new TableHealth(5, 40, 30, 0, 1000, 123456, 4, 1_700_000_000_000L, -1), 1_000_000);
        String scrape = registry.scrape();

        // One contains(...) call: chaining on AssertJ's SELF type trips the IDE's null analysis.
        assertThat(scrape).contains(
                "iceberg_table_up{table=\"lakehouse.events\"} 1.0",
                "iceberg_table_small_data_files{table=\"lakehouse.events\"} 30.0",
                "iceberg_table_last_commit_timestamp_seconds{table=\"lakehouse.events\"} 1.7E9",
                // Never compacted: NaN, so an alert on time() - this value cannot fire by accident.
                "iceberg_table_last_compaction_timestamp_seconds{table=\"lakehouse.events\"} NaN",
                "iceberg_exporter_inspections_total{result=\"success\",table=\"lakehouse.events\"} 1.0");
    }

    @Test
    void exportsNumericLakehouseSnapshotProperties() {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        TableMetrics metrics = new TableMetrics(registry, "lakehouse.events_reconciliation");
        Map<String, Double> first = TableHealth.numericProperties(Map.of(
                "lakehouse.missing", "3", "lakehouse.checked-offsets", "1000", "lakehouse.note", "text",
                "added-records", "1"));

        metrics.record(new TableHealth(1, 1, 1, 0, 1, 100, 1, 1_700_000_000_000L, -1, first), 1_000_000);
        assertThat(registry.scrape()).contains(
                "iceberg_table_snapshot_property{property=\"lakehouse.missing\",table=\"lakehouse.events_reconciliation\"} 3.0",
                "iceberg_table_snapshot_property{property=\"lakehouse.checked-offsets\",table=\"lakehouse.events_reconciliation\"} 1000.0");
        assertThat(first).doesNotContainKeys("lakehouse.note", "added-records");

        // The next snapshot lacks a key: it reads NaN rather than the old value.
        metrics.record(new TableHealth(2, 1, 1, 0, 1, 100, 1, 1_700_000_100_000L, -1,
                Map.of("lakehouse.missing", 0.0)), 1_000_000);
        assertThat(registry.scrape()).contains(
                "iceberg_table_snapshot_property{property=\"lakehouse.missing\",table=\"lakehouse.events_reconciliation\"} 0.0",
                "iceberg_table_snapshot_property{property=\"lakehouse.checked-offsets\",table=\"lakehouse.events_reconciliation\"} NaN");
    }

    @Test
    void aFailedInspectionMarksTheTableDownButKeepsLastValues() {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        TableMetrics metrics = new TableMetrics(registry, "lakehouse.events");
        metrics.record(new TableHealth(5, 40, 30, 0, 1000, 123456, 4, 1_700_000_000_000L, -1), 1_000_000);

        metrics.failed();
        String scrape = registry.scrape();

        assertThat(scrape).contains(
                "iceberg_table_up{table=\"lakehouse.events\"} 0.0",
                "iceberg_table_data_files{table=\"lakehouse.events\"} 40.0",
                "iceberg_exporter_inspections_total{result=\"failure\",table=\"lakehouse.events\"} 1.0");
    }

    @Test
    void configReadsTablesAndCatalog() {
        ExporterConfig config = ExporterConfig.fromEnv(Map.of(
                "ICEBERG_CATALOG_TYPE", "glue", "TABLES", "lakehouse.events, lakehouse.events_hourly"));

        assertThat(config.catalogImpl()).isEqualTo(ExporterConfig.GLUE_CATALOG);
        assertThat(config.tables()).containsExactly("lakehouse.events", "lakehouse.events_hourly");
        assertThat(config.catalogProperties()).doesNotContainKey("uri");
        assertThat(config.smallFileBytes()).isEqualTo(32L * 1024 * 1024);
        assertThatThrownBy(() -> ExporterConfig.fromEnv(Map.of("TABLES", " ")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
