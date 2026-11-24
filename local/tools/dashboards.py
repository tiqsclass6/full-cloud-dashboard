#!/usr/bin/env python3
"""Generate the platform's Grafana dashboards (dashboards as code).

  python local/tools/dashboards.py            write helm/charts/observability/dashboards/*.json
  python local/tools/dashboards.py --check    exit 1 if the committed JSON differs
"""
import argparse
import json
import pathlib
import sys

OUT = pathlib.Path(__file__).resolve().parents[2] / "helm" / "charts" / "observability" / "dashboards"
PROM = {"type": "prometheus", "uid": "prometheus"}
LOKI = {"type": "loki", "uid": "loki"}
SLO = 0.995                                   # ingest availability objective


class Layout:
    """Places panels left to right in a 24-column grid, wrapping to a new row when full."""

    def __init__(self):
        self.x = self.y = self.row_h = 0
        self.next_id = 1

    def place(self, panel, w, h):
        if self.x + w > 24:
            self.x, self.y, self.row_h = 0, self.y + self.row_h, 0
        panel["gridPos"] = {"x": self.x, "y": self.y, "w": w, "h": h}
        panel["id"] = self.next_id
        self.next_id += 1
        self.x += w
        self.row_h = max(self.row_h, h)
        return panel

    def row(self, title):
        if self.x:
            self.x, self.y, self.row_h = 0, self.y + self.row_h, 0
        panel = {"type": "row", "title": title, "collapsed": False, "panels": []}
        self.place(panel, 24, 1)
        self.x, self.y, self.row_h = 0, self.y + 1, 0
        return panel


def targets(queries, datasource=PROM):
    return [{"datasource": datasource, "expr": q, "legendFormat": legend, "refId": chr(65 + i)}
            for i, (q, legend) in enumerate(queries)]


def thresholds(steps):
    """steps: [(value or None, colour), ...] from the base upwards."""
    return {"mode": "absolute", "steps": [{"value": v, "color": c} for v, c in steps]}


def stat(title, query, unit="none", steps=None, description="", decimals=None):
    defaults = {"unit": unit, "thresholds": thresholds(steps or [(None, "green")])}
    if decimals is not None:
        defaults["decimals"] = decimals
    return {"type": "stat", "title": title, "description": description, "datasource": PROM,
            "targets": targets([(query, "")]),
            "fieldConfig": {"defaults": defaults, "overrides": []},
            "options": {"colorMode": "background", "graphMode": "area", "reduceOptions":
                        {"calcs": ["lastNotNull"], "fields": "", "values": False}, "textMode": "value"}}


def timeseries(title, queries, unit="none", description="", stack=False, steps=None, min_zero=True):
    custom = {"lineWidth": 2, "fillOpacity": 25 if stack else 10, "showPoints": "never",
              "stacking": {"mode": "normal" if stack else "none", "group": "A"}}
    defaults = {"unit": unit, "custom": custom}
    if min_zero:
        defaults["min"] = 0
    if steps:
        defaults["thresholds"] = thresholds(steps)
        custom["thresholdsStyle"] = {"mode": "line+area"}
    return {"type": "timeseries", "title": title, "description": description, "datasource": PROM,
            "targets": targets(queries), "fieldConfig": {"defaults": defaults, "overrides": []},
            "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                        "tooltip": {"mode": "multi", "sort": "desc"}}}


def table(title, queries, columns, description=""):
    """Instant queries joined by labels into one row per series (e.g. one row per Iceberg table)."""
    t = targets(queries)
    for target in t:
        target.update({"instant": True, "format": "table"})
    return {"type": "table", "title": title, "description": description, "datasource": PROM, "targets": t,
            "transformations": [{"id": "merge", "options": {}},
                                {"id": "organize", "options": {"excludeByName": {"Time": True},
                                                               "renameByName": columns}}],
            "fieldConfig": {"defaults": {}, "overrides": []}, "options": {"showHeader": True}}


def logs(title, query, description=""):
    return {"type": "logs", "title": title, "description": description, "datasource": LOKI,
            "targets": [{"datasource": LOKI, "expr": query, "refId": "A"}],
            "options": {"showTime": True, "wrapLogMessage": True, "sortOrder": "Descending",
                        "enableLogDetails": True}}


def dashboard(uid, title, description, build):
    layout = Layout()
    panels = []
    build(layout, panels)
    return {"uid": uid, "title": title, "description": description, "tags": ["lakehouse"],
            "timezone": "utc", "schemaVersion": 39, "version": 1, "editable": False,
            "time": {"from": "now-6h", "to": "now"}, "refresh": "30s",
            "links": [{"title": "Lakehouse dashboards", "type": "dashboards", "tags": ["lakehouse"],
                       "asDropdown": True}],
            "templating": {"list": []}, "annotations": {"list": []}, "panels": panels}


ERRORS = '(sum(rate(ingest_requests_total{outcome=~"overloaded|failed|error"}[%s])) or vector(0))'
ALL = 'sum(rate(ingest_requests_total[%s]))'
GREEN_RED = [(None, "red"), (1, "green")]


def overview(L, P):
    P.append(L.row("Service level"))
    P.append(L.place(stat("Ingest availability (1d)", f"1 - ({ERRORS % '1d'} / {ALL % '1d'})", "percentunit",
                          [(None, "red"), (SLO, "green")], "Objective: 99.5% of requests succeed.", 3), 5, 5))
    P.append(L.place(stat("Error budget left (1d)",
                          f"1 - (({ERRORS % '1d'} / {ALL % '1d'}) / {1 - SLO:.3f})", "percentunit",
                          [(None, "red"), (0.25, "orange"), (0.5, "green")],
                          "Share of the 0.5% budget not yet used over the last day.", 1), 5, 5))
    P.append(L.place(stat("Data freshness (events)",
                          'min(time() - iceberg_table_last_commit_timestamp_seconds{table="lakehouse.events"})', "s",
                          [(None, "green"), (120, "orange"), (300, "red")],
                          "Age of the newest Iceberg snapshot. Flink commits every 10 s."), 5, 5))
    P.append(L.place(stat("Records missing", f"{prop('missing')}", "none", [(None, "green"), (1, "red")],
                          "Kafka offsets the last reconciliation found in neither events nor events_skipped "
                          "(Lakehouse / Integrity).", 0), 5, 5))
    P.append(L.place(stat("Alerts firing", 'count(ALERTS{alertstate="firing", alertname!="Watchdog"}) or vector(0)',
                          "none", [(None, "green"), (1, "red")]), 4, 5))
    P.append(L.row("Pipeline"))
    P.append(L.place(timeseries("Events in (per second)", [
        ('sum(rate(ingest_events_total{outcome="published"}[2m])) or vector(0)', "published to Kafka"),
        ('sum(rate(ingest_events_total{outcome!="published"}[2m])) or vector(0)', "not published")], "short"), 12, 8))
    P.append(L.place(timeseries("Flink consumer lag (events)", [
        ('sum(kafka_consumergroup_lag{consumergroup="flink-events-to-iceberg"} >= 0)', "lag")], "short"), 12, 8))
    P.append(L.place(stat("Flink job up", 'max(flink_jobmanager_job_uptime{job_name="events_to_iceberg"}) > bool 0',
                          "none", GREEN_RED), 6, 4))
    P.append(L.place(stat("Kafka brokers up", 'count(up{job="monitoring/kafka-brokers", pod!~".+-controllers-[0-9]+"} == 1)', "none",
                          [(None, "red"), (3, "green")]), 6, 4))
    P.append(L.place(stat("Argo CD apps healthy", 'count(argocd_app_info{health_status="Healthy"})', "none"), 6, 4))
    P.append(L.place(stat("Argo CD apps not synced", 'count(argocd_app_info{sync_status!="Synced"}) or vector(0)',
                          "none", [(None, "green"), (1, "orange")]), 6, 4))


def ingestion(L, P):
    P.append(L.row("Availability SLO (99.5%)"))
    P.append(L.place(timeseries("Requests by outcome", [
        ('sum by (outcome) (rate(ingest_requests_total[2m]))', "{{outcome}}")], "reqps", stack=True), 12, 8))
    P.append(L.place(timeseries("Error ratio against the objective", [
        ("slo:ingest_requests:error_ratio_rate5m", "5m"), ("slo:ingest_requests:error_ratio_rate1h", "1h"),
        ("slo:ingest_requests:error_ratio_rate6h", "6h")], "percentunit",
        "Failures (overloaded, failed, error) as a share of requests; the budget is 0.5%.",
        steps=[(None, "green"), (1 - SLO, "red")]), 12, 8))
    P.append(L.place(timeseries("Burn rate (1 = budget used exactly over 28 days)", [
        (f"slo:ingest_requests:error_ratio_rate1h / {1 - SLO:.3f}", "1h"),
        (f"slo:ingest_requests:error_ratio_rate6h / {1 - SLO:.3f}", "6h"),
        (f"slo:ingest_requests:error_ratio_rate1d / {1 - SLO:.3f}", "1d")], "none",
        "Fast-burn page above 14.4 (1h) or 6 (6h); slow-burn ticket above 3 (1d).",
        steps=[(None, "green"), (6, "orange"), (14.4, "red")]), 24, 7))
    P.append(L.row("Latency and capacity"))
    P.append(L.place(timeseries("Request latency", [
        ('histogram_quantile(0.5, sum by (le) (rate(ingest_request_latency_seconds_bucket[5m])))', "p50"),
        ('histogram_quantile(0.99, sum by (le) (rate(ingest_request_latency_seconds_bucket[5m])))', "p99")],
        "s", steps=[(None, "green"), (0.5, "red")]), 12, 8))
    P.append(L.place(timeseries("Kafka produce latency", [
        ('histogram_quantile(0.5, sum by (le) (rate(ingest_produce_latency_seconds_bucket[5m])))', "p50"),
        ('histogram_quantile(0.99, sum by (le) (rate(ingest_produce_latency_seconds_bucket[5m])))', "p99")],
        "s"), 12, 8))
    P.append(L.place(timeseries("In-flight events (backpressure)", [
        ("sum(ingest_inflight)", "in flight"), ("sum(ingest_inflight_max)", "limit")], "short"), 12, 8))
    P.append(L.place(timeseries("JVM heap and GC", [
        ('sum by (pod) (jvm_memory_used_bytes{job="ingestion-service", area="heap"})', "heap {{pod}}"),
        ('sum by (pod) (rate(jvm_gc_pause_seconds_sum{job="ingestion-service"}[5m]))', "GC s/s {{pod}}")],
        "bytes"), 12, 8))


def pipeline(L, P):
    flink = '{job_name="events_to_iceberg"}'
    P.append(L.row("Flink job"))
    P.append(L.place(stat("Uptime", f"max(flink_jobmanager_job_uptime{flink}) / 1000", "s"), 6, 5))
    P.append(L.place(stat("Restarts (1h)", f"sum(increase(flink_jobmanager_job_numRestarts{flink}[1h]))", "none",
                          [(None, "green"), (1, "orange"), (3, "red")], decimals=0), 6, 5))
    P.append(L.place(stat("Checkpoints completed (1h)",
                          f"sum(increase(flink_jobmanager_job_numberOfCompletedCheckpoints{flink}[1h]))", "none",
                          decimals=0), 6, 5))
    P.append(L.place(stat("Checkpoints failed (1h)",
                          f"sum(increase(flink_jobmanager_job_numberOfFailedCheckpoints{flink}[1h]))", "none",
                          [(None, "green"), (1, "red")], decimals=0), 6, 5))
    P.append(L.place(timeseries("Checkpoint duration and size", [
        (f"max(flink_jobmanager_job_lastCheckpointDuration{flink}) / 1000", "duration (s)"),
        (f"max(flink_jobmanager_job_lastCheckpointSize{flink}) / 1048576", "size (MiB)")], "short"), 12, 8))
    P.append(L.place(timeseries("Back-pressured tasks", [
        (f"sum by (task_name) (flink_taskmanager_job_task_isBackPressured{flink})", "{{task_name}}")],
        "short", "1 = the task waits on its downstream operator."), 12, 8))
    P.append(L.row("Kafka to Iceberg"))
    P.append(L.place(timeseries("Consumer lag by partition", [
        ('kafka_consumergroup_lag{consumergroup="flink-events-to-iceberg"} >= 0', "partition {{partition}}")],
        "short", stack=True), 12, 8))
    P.append(L.place(timeseries("Iceberg data freshness", [
        ('min(time() - iceberg_table_last_commit_timestamp_seconds{table="lakehouse.events"})', "seconds since last commit")],
        "s", steps=[(None, "green"), (300, "red")]), 12, 8))
    P.append(L.place(logs("Flink JobManager logs (warnings and errors)",
                          '{namespace="apps", app="events-to-iceberg"} |~ "WARN|ERROR|Exception"'), 24, 9))


def kafka(L, P):
    P.append(L.row("Cluster health"))
    P.append(L.place(stat("Brokers up", 'count(up{job="monitoring/kafka-brokers", pod!~".+-controllers-[0-9]+"} == 1)', "none",
                          [(None, "red"), (3, "green")]), 6, 5))
    P.append(L.place(stat("Under-replicated", "sum(kafka_server_replicamanager_underreplicatedpartitions)", "none",
                          [(None, "green"), (1, "orange")]), 6, 5))
    P.append(L.place(stat("Under min ISR", "sum(kafka_server_replicamanager_underminisrpartitioncount)", "none",
                          [(None, "green"), (1, "red")]), 6, 5))
    P.append(L.place(stat("Active controllers", "sum(kafka_controller_kafkacontroller_activecontrollercount)",
                          "none", [(None, "red"), (1, "green"), (2, "red")]), 6, 5))
    P.append(L.row("Traffic"))
    P.append(L.place(timeseries("Messages in by topic", [
        ('sum by (topic) (rate(kafka_server_brokertopicmetrics_messagesinpersec_total{topic!=""}[2m]))', "{{topic}}")],
        "short"), 12, 8))
    P.append(L.place(timeseries("Bytes in and out", [
        ('sum(rate(kafka_server_brokertopicmetrics_bytesinpersec_total{topic=""}[2m]))', "in"),
        ('sum(rate(kafka_server_brokertopicmetrics_bytesoutpersec_total{topic=""}[2m]))', "out")], "Bps"), 12, 8))
    P.append(L.place(timeseries("Consumer group lag", [
        ("sum by (consumergroup) (kafka_consumergroup_lag >= 0)", "{{consumergroup}}")], "short"), 12, 8))
    P.append(L.place(timeseries("Partitions per broker", [
        ("sum by (pod) (kafka_server_replicamanager_partitioncount)", "{{pod}}")], "short"), 12, 8))


def tables(L, P):
    P.append(L.row("Iceberg tables"))
    P.append(L.place(table("Table health", [
        ("iceberg_table_records", ""), ("iceberg_table_data_files", ""), ("iceberg_table_small_data_files", ""),
        ("iceberg_table_snapshots", ""), ("time() - iceberg_table_last_commit_timestamp_seconds", ""),
        ("time() - iceberg_table_last_compaction_timestamp_seconds", "")],
        {"table": "Table", "Value #A": "Rows", "Value #B": "Data files", "Value #C": "Small files",
         "Value #D": "Snapshots", "Value #E": "Last commit (s ago)", "Value #F": "Last compaction (s ago)"}), 24, 7))
    P.append(L.place(timeseries("Small files (compaction candidates)", [
        ("max by (table) (iceberg_table_small_data_files)", "{{table}}")], "short"), 12, 8))
    P.append(L.place(timeseries("Snapshots kept (expiry trims these)", [
        ("max by (table) (iceberg_table_snapshots)", "{{table}}")], "short"), 12, 8))
    P.append(L.place(timeseries("Rows", [("max by (table) (iceberg_table_records)", "{{table}}")], "short"), 12, 8))
    P.append(L.place(timeseries("Table size", [("max by (table) (iceberg_table_size_bytes)", "{{table}}")], "bytes"), 12, 8))
    P.append(L.row("Spark jobs"))
    P.append(L.place(stat("Succeeded (24h)", "sum(increase(spark_application_success_count[24h]))", "none",
                          decimals=0), 8, 5))
    P.append(L.place(stat("Failed (24h)", "sum(increase(spark_application_failure_count[24h])) or vector(0)", "none",
                          [(None, "green"), (1, "red")], decimals=0), 8, 5))
    P.append(L.place(stat("Exporter health", "min(iceberg_table_up)", "none", GREEN_RED,
                          "1 when every table was inspected successfully."), 8, 5))


RECONCILED = 'table="lakehouse.events_reconciliation"'
FLINK_JOB = 'job_name="events_to_iceberg"'


def prop(name):
    """A total the last reconciliation wrote into its snapshot summary (iceberg-exporter)."""
    return f'max(iceberg_table_snapshot_property{{{RECONCILED}, property="lakehouse.{name}"}})'


def integrity(L, P):
    P.append(L.row("Last reconciliation (events-reconcile, hourly, last 24 hours)"))
    P.append(L.place(stat("Missing offsets", prop("missing"), "none", [(None, "green"), (1, "red")],
                          "Offsets in neither events nor events_skipped: accepted by Kafka, not in the lakehouse. "
                          "The run's missing_ranges column in events_reconciliation lists them; "
                          "recover with local/scripts/replay.sh (runbook lakehouse.md#lakehouserecordsmissing).", 0),
                     6, 5))
    P.append(L.place(stat("Offsets stored twice", prop("duplicated-offsets"), "none", [(None, "green"), (1, "red")],
                          "The same partition and offset in the lakehouse more than once (LakehouseRecordsDuplicated).",
                          0), 6, 5))
    P.append(L.place(stat("Offsets checked", prop("checked-offsets"), "none", description=
                          "Every offset between each partition's first and last in the window.", decimals=0), 6, 5))
    P.append(L.place(stat("Last run", f"time() - max(iceberg_table_last_commit_timestamp_seconds{{{RECONCILED}}})",
                          "s", [(None, "green"), (7200, "orange"), (10800, "red")],
                          "Age of the last run. LakehouseReconciliationStale fires after 3 missed runs of a scheduled "
                          "job; one-off runs (and kind, where the schedule is suspended) do not arm it."), 6, 5))
    P.append(L.row("Where the offsets went (last run)"))
    P.append(L.place(stat("Replayed", prop("replayed"), "none", description=
                          "Offsets accounted for by a replay through the API (attribute lakehouse.replayed_from).",
                          decimals=0), 6, 5))
    P.append(L.place(stat("Skipped: duplicate copies", prop("skipped-duplicate"), "none", description=
                          "Retried copies Flink dropped; each is a row of events_skipped.", decimals=0), 6, 5))
    P.append(L.place(stat("Skipped: undecodable", prop("skipped-undecodable"), "none",
                          [(None, "green"), (1, "orange")],
                          "Records Flink could not decode, kept with their bytes in events_skipped "
                          "(runbook pipeline.md#flinkrecordsundecodable).", 0), 6, 5))
    P.append(L.place(stat("Duplicate event ids", prop("duplicate-event-ids"), "none", [(None, "green"), (1, "orange")],
                          "An event_id stored twice at different offsets: a retry later than the 60-minute "
                          "de-duplication window. A limit of the retry contract; not alerted.", 0), 6, 5))
    P.append(L.place(timeseries("Problems found per run", [
        (prop("missing"), "missing"), (prop("duplicated-offsets"), "stored twice"),
        (prop("duplicate-event-ids"), "duplicate event ids")], "short",
        "Each step is a reconciliation run; the values hold until the next one."), 12, 8))
    P.append(L.place(timeseries("Offsets per run", [
        (prop("checked-offsets"), "checked"), (prop("skipped-duplicate"), "skipped: duplicate"),
        (prop("skipped-undecodable"), "skipped: undecodable"), (prop("replayed"), "replayed")], "short"), 12, 8))
    P.append(L.row("Flink: records set aside"))
    P.append(L.place(timeseries("Set aside per 5 minutes", [
        (f"sum(increase(flink_taskmanager_job_task_operator_deserializer_deserializationErrors{{{FLINK_JOB}}}[5m]))",
         "undecodable"),
        (f"sum(increase(flink_taskmanager_job_task_operator_duplicatesDropped{{{FLINK_JOB}}}[5m]))",
         "duplicate copies")], "short",
        "Live counters from the job; the reconciliation confirms them later from the tables."), 12, 8))
    P.append(L.place(stat("Undecodable (15m)",
                          f"sum(increase(flink_taskmanager_job_task_operator_deserializer_deserializationErrors"
                          f"{{{FLINK_JOB}}}[15m])) or vector(0)", "none", [(None, "green"), (1, "orange")],
                          "FlinkRecordsUndecodable fires above 0.", 0), 6, 8))
    P.append(L.place(stat("Duplicate copies dropped (1h)",
                          f"sum(increase(flink_taskmanager_job_task_operator_duplicatesDropped{{{FLINK_JOB}}}[1h]))"
                          " or vector(0)", "none", description="Retries with an event_id Flink had already seen.",
                          decimals=0), 6, 8))
    P.append(L.place(logs("Undecodable records (Flink log)",
                          '{namespace="apps", app="events-to-iceberg"} |= "Undecodable record at"',
                          "One line per record set aside, with its partition, offset and error."), 24, 8))


DASHBOARDS = {
    "overview": ("lakehouse-overview", "Lakehouse / Overview",
                 "Service level, pipeline flow and platform health at a glance.", overview),
    "ingestion": ("lakehouse-ingestion", "Lakehouse / Ingestion SLO",
                  "Availability SLO (99.5%), burn rates, latency and capacity of the ingestion service.", ingestion),
    "pipeline": ("lakehouse-pipeline", "Lakehouse / Pipeline",
                 "Flink job health, checkpoints, backpressure, Kafka lag and Iceberg freshness.", pipeline),
    "kafka": ("lakehouse-kafka", "Lakehouse / Kafka",
              "Replication health, controller quorum and traffic of the Strimzi Kafka cluster.", kafka),
    "tables": ("lakehouse-tables", "Lakehouse / Tables",
               "Iceberg table health (iceberg-exporter) and the Spark maintenance jobs.", tables),
    "integrity": ("lakehouse-integrity", "Lakehouse / Integrity",
                  "Every Kafka offset accounted for: the hourly reconciliation, replays, and records Flink set aside.",
                  integrity),
}


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--check", action="store_true", help="report drift instead of writing")
    args = parser.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    drift = []
    for name, (uid, title, description, build) in DASHBOARDS.items():
        text = json.dumps(dashboard(uid, title, description, build), indent=2, sort_keys=True) + "\n"
        path = OUT / f"{name}.json"
        if args.check:
            if not path.exists() or path.read_text() != text:
                drift.append(path.name)
        else:
            path.write_text(text)
            print(f"wrote {path.relative_to(OUT.parents[3])}")
    if drift:
        print("dashboards out of date (run local/tools/dashboards.py):", ", ".join(drift))
        sys.exit(1)


if __name__ == "__main__":
    main()
