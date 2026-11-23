---
title: Observability
layout: default
---

## **Observability**

Metrics, logs and traces for every component, a service level objective with burn-rate alerts, dashboards and alert rules as code, and runbooks for every alert. Design decisions: [**ADR 0009**](architecture-decisions/0009-observability-and-chaos.md). Behaviour under failure: [**chaos experiments**](chaos/README.md).

## **Signals**

| **Signal**  | **Collected from**                                                                                                                                                              | **Stored in**                      |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------- |
| **Metrics** | Kafka (Strimzi), ingestion service (Micrometer), Flink (Prometheus reporter), Spark operator, Iceberg tables (iceberg-exporter), Kyverno, Argo CD, External Secrets, Kubernetes | Prometheus (kube-prometheus-stack) |
| **Logs**    | Every pod's stdout, read through the Kubernetes API by Grafana Alloy (no hostPath, non-root)                                                                                    | Loki (72 h)                        |
| **Traces**  | The ingestion service (OpenTelemetry Java agent): HTTP request, schema registry call, Kafka produce                                                                             | Tempo (72 h)                       |

Grafana links them: a span opens the logs of the pod that produced it; Tempo's metrics generator adds a service graph.

## **Service level objectives**

| **SLO**                 | **Objective**                                        | **Alerting**                                                                                                    |
| ----------------------- | ---------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| **Ingest availability** | 99.5% of `/v1/events` requests succeed (28 days)     | Burn rate: page at 14.4x (1 h and 5 min) or 6x (6 h and 30 min); ticket at 3x (1 d and 2 h) or 1x (3 d and 6 h) |
| **Ingest latency**      | p99 below 500 ms                                     | `IngestLatencyHigh` after 10 minutes                                                                            |
| **Data freshness**      | The events table committed within the last 5 minutes | `IcebergTableStale` (the Flink sink commits every 10 seconds)                                                   |

Client mistakes (invalid, malformed or oversized requests) do not spend the error budget; load shedding, Kafka failures and unexpected errors do.

## **Alerts**

31 alerts in six groups (`helm/charts/observability/rules`), each with a runbook:

| **Group**               | **Alerts**                                                                                                                                                                        | **Runbook**                                                                        |
| ----------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| **Ingestion SLO**       | IngestErrorBudgetBurnFast, IngestErrorBudgetBurnSlow, IngestLatencyHigh                                                                                                           | [**ingestion.md**](runbooks/ingestion.md)                                          |
| **Pipeline**            | IcebergTableStale, FlinkJobNotRunning, FlinkCheckpointsStalled, FlinkCheckpointFailures, FlinkRecordsUndecodable, FlinkJobRestarting, FlinkBackpressureHigh, KafkaConsumerLagHigh | [**pipeline.md**](runbooks/pipeline.md)                                            |
| **Kafka**               | KafkaBrokerDown, KafkaUnderReplicatedPartitions, KafkaUnderMinIsr, KafkaOfflinePartitions, KafkaNoActiveController                                                                | [**kafka.md**](runbooks/kafka.md)                                                  |
| **Integrity**           | LakehouseRecordsMissing, LakehouseRecordsDuplicated, LakehouseReconciliationStale ([**results**](integrity/README.md))                                                            | [**lakehouse.md**](runbooks/lakehouse.md)                                          |
| **Ops agent**           | OpsAgentComponentDown, OpsAgentInvestigationsFailing, OpsAgentAlertsDropped, OpsControlActionFailed                                                                               | [**ai-ops.md**](runbooks/ai-ops.md)                                                |
| **Tables and platform** | IcebergExporterDown, IcebergSmallFilesHigh, IcebergCompactionOverdue, IcebergSnapshotsAccumulating, SparkJobFailed, ArgoCDAppOutOfSync, ArgoCDAppDegraded, ExternalSecretNotReady | [**lakehouse.md**](runbooks/lakehouse.md), [**platform.md**](runbooks/platform.md) |

Alertmanager sends alerts to the ops agent ([**ai-ops.md**](ai-ops.md)), except its own alerts (label `ops_agent: skip`). A direct route to people (pager, email) is not configured.

## **Dashboards**

Generated by `local/tools/dashboards.py` (`--check` reports drift) into `helm/charts/observability/dashboards`, loaded by Grafana's sidecar:

| **Dashboard**                 | **Shows**                                                                                                                                       |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| **Lakehouse / Overview**      | Availability and error budget, freshness, records missing, alerts, event flow, Flink, Kafka, Argo CD                                            |
| **Lakehouse / Ingestion SLO** | Requests by outcome, error ratio against the objective, burn rates, latency, in-flight, JVM                                                     |
| **Lakehouse / Pipeline**      | Flink uptime, restarts, checkpoints, backpressure, consumer lag, freshness, Flink logs (Loki)                                                   |
| **Lakehouse / Kafka**         | Brokers, replication, controller quorum, traffic by topic, consumer lag                                                                         |
| **Lakehouse / Tables**        | Iceberg table health (files, small files, snapshots, commit and compaction age), Spark jobs                                                     |
| **Lakehouse / Integrity**     | The last reconciliation (missing, stored twice, checked, age), where offsets went (replayed, skipped), records Flink set aside, their log lines |

## **Checks**

| **Command**                       | **Checks**                                                                                   |
| --------------------------------- | -------------------------------------------------------------------------------------------- |
| **`make rules-test`**             | `promtool check rules` and 19 unit tests (exact labels and annotations, must-not-fire cases) |
| **`make dashboards`**             | Regenerate the dashboards; `python local/tools/dashboards.py --check` fails on drift         |
| **`make chaos EXP=<experiment>`** | One chaos experiment with alert timeline, recovery time and exactly-once verification        |

## **Verified (2026-10-31, local)**

| **Check**                                                                                   | **Result**                                                                                                                          |
| ------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| **Scrape targets for every new source (Flink, Spark operator, Kyverno, Argo CD, exporter)** | All up, none down                                                                                                                   |
| **Every dashboard query run against Prometheus**                                            | 57 of 57 return data (the first run found 12 empty: two real bugs, below)                                                           |
| **Rule unit tests (`make rules-test`)**                                                     | 13 of 13 pass; mutation checks confirmed the tests fail when a rule is broken                                                       |
| **Logs**                                                                                    | Pod logs from all 13 namespaces in Loki                                                                                             |
| **Traces**                                                                                  | One ingest request: an HTTP server span with one Kafka producer span per event; scrape and probe spans dropped                      |
| **An alert end to end**                                                                     | `IcebergCompactionOverdue` fired (701 small files); its runbook step (compaction) ran; small files fell to 4 and the alert resolved |

What building this found:

- **The SLO error ratio was empty instead of 0 when nothing failed** (no "bad" series exists, so the division returned nothing): the SLO panels would have been blank exactly when all was well. Fixed with `or vector(0)` and a test.
- **Two Kafka metric names were wrong** (the JMX names end in `persec`): found by running every dashboard query.
- **Flink's reporter writes the job name with underscores** (`events_to_iceberg`), so the first Flink rules would never have matched.
- **The alert runbook found two regressions from Phase 7:** `spark-run.sh` (used by runbooks and the repair scripts) and the load-test Job built their own pod specs without the settings Pod Security `restricted` requires, so both were rejected. Both are fixed and admitted again.

## **Grafana**

`make local-ports` forwards Grafana to `http://localhost:3000` (`admin` / `admin-local-dev`, local only). On AWS the admin password comes from Secrets Manager.
