# **Runbook: ingestion service**

Alerts from `helm/charts/observability/rules/slo-ingest.yaml`. Dashboard: **Lakehouse / Ingestion SLO**.

**SLO:** 99.5% of requests to `/v1/events` succeed over 28 days (error budget 0.5%). Client mistakes (`invalid`, `malformed`, `too_large`, `method_not_allowed`) are not failures; `overloaded` (load shedding), `failed` (Kafka did not acknowledge) and `error` are.

## **IngestErrorBudgetBurnFast**

**Meaning:** failures are burning the budget at 14 times the sustainable rate over the last hour (and still in the last 5 minutes), or 6 times over 6 hours (and the last 30 minutes).
**Impact:** clients are getting errors now; at this rate a month's budget is gone in about two days.
**Diagnose:** which outcome? Dashboard "Requests by outcome", or `sum by (outcome) (rate(ingest_requests_total[5m]))`.

- `failed`: Kafka is not acknowledging: see [**kafka.md**](kafka.md) (KafkaUnderMinIsr, KafkaBrokerDown). Ingestion logs: `{namespace="apps", app="ingestion-service"} |= "Exception"`.
- `overloaded`: in-flight limit reached (`ingest_inflight` against `ingest_inflight_max`): more traffic than capacity, or a slow Kafka making each request hold its slot longer.
- `error`: unexpected exceptions: logs, and the trace of a failing request in Tempo.
**Fix:** for Kafka, restore the cluster. For overload, scale (`ingestion-service.autoscaling` or `replicaCount` in Git) after checking Kafka latency is normal.

## **IngestErrorBudgetBurnSlow**

**Meaning:** a lower but sustained failure rate (3 times over a day, or 1 time over 3 days).
**Impact:** the budget will run out before the window ends.
**Fix:** not urgent; find the source of steady failures (often one client sending too-large batches, or periodic Kafka leader moves) during working hours.

## **IngestLatencyHigh**

**Meaning:** p99 request latency above 500 ms for 10 minutes.
**Diagnose:** "Produce latency" against "Request latency" on the dashboard: if produce latency dominates, Kafka is slow (disk, replication after a broker restart). A slow trace in Tempo shows where the time goes (schema registry, Kafka send).
**Fix:** depends on the slow component; JVM GC pauses show in the JVM panels (G1 is configured; see docs/performance).
