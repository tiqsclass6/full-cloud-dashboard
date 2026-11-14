# **ADR 0009: Observability hardening and chaos engineering (Phase 8)**

- Status: accepted
- Date: 2026-10-31

## **Context**
After Phase 7 the platform exported metrics from Kafka and the ingestion service only. The Flink job (the component that decides freshness and correctness), Spark, the Iceberg tables themselves and the platform's control plane were invisible; there were no alert rules of our own, no dashboards, no logs or traces, and no evidence of how the platform behaves when something breaks.

## **Decisions**
1. **Metrics from every moving part.**
   - Flink: its Prometheus reporter (the plugin ships in the image).
   - Spark: the operator's job metrics. Batch jobs are too short-lived to scrape reliably.
   - Platform: Kyverno, Argo CD and External Secrets.
   - Tables: a purpose-built **Iceberg table-health exporter** (Java, Micrometer), because no existing exporter reports small files, snapshot growth, freshness and compaction age.
2. **The exporter exports timestamps, not ages.** Alerts compute `time() - …`, so a stale exporter cannot make data look fresh. A table that was never compacted reports `NaN`, which no comparison matches, so it cannot fire a compaction alert.
3. **One SLO with burn-rate alerting, plus symptom alerts.**
   - Ingest availability (99.5%) uses multi-window, multi-burn-rate alerts (Google SRE workbook) instead of a static error threshold.
   - Freshness is a direct symptom: the Flink sink commits on every checkpoint, so the age of the newest Iceberg snapshot is the freshness signal.
   - Client mistakes (invalid, malformed) do not spend the error budget.
4. **Rules are code with tests.**
   - Rule files stay in plain Prometheus format inside the chart, wrapped into `PrometheusRule` objects at render time, so `promtool` checks and unit-tests the exact files that are deployed.
   - Tests state exact labels and annotations, include must-not-fire cases, and were mutation-checked (breaking a rule makes a test fail).
5. **Dashboards are generated.** `local/tools/dashboards.py` declares panels compactly and writes the JSON. A `--check` mode detects drift, and every query in every dashboard is executed against Prometheus as a check.
6. **Logs and traces through one collector.**
   - Grafana Alloy collects pod logs **through the Kubernetes API**, with no hostPath and no privileged agent; it runs as a non-root user and meets Pod Security `restricted`.
   - It receives OTLP traces and drops scrape and probe spans.
   - Loki and Tempo run as single binaries with volume storage, the right scale for one cluster.
   - The ingestion service is traced by the OpenTelemetry Java agent, pinned by version and checksum. The trace context travels in Kafka headers.
7. **Chaos Mesh, but never through GitOps.**
   - Experiments are versioned in `chaos/experiments` but applied only by `local/scripts/chaos-run.sh`. A sync must never start a fault.
   - Chaos Mesh may only target namespaces explicitly annotated as chaos targets.
   - The harness always removes the fault on exit, verifies every event id end to end, and records the alert timeline.
8. **Faults that respect the admission policies.** Chaos Mesh's `pod-failure` rewrites a pod's image, which the Phase 7 registry policy rightly rejects. The experiments use network loss, latency, pod kills and a paused node instead, which leave pod specs alone.

## **Consequences**
- The pipeline's health can be read from Grafana and Loki alone, and a request can be followed from HTTP into Kafka in Tempo.
- Charts that generate their own TLS material at render time do not suit GitOps as-is: every refresh produces a new CA. The operators ApplicationSet ignores those fields and keeps the first applied values (seen with Chaos Mesh).
- The chaos daemon is privileged (it enters pods' network and process namespaces). It is a test tool, confined to non-production clusters and to the opted-in namespaces.
- On kind, Docker Desktop's VM clock correction makes every process start time jitter, which kube-prometheus-stack's crash-loop alert reads as restarts. That one rule is disabled locally, not on AWS.
- The OpenTelemetry agent adds overhead to the ingestion path; sampling is 100% locally and 10% in dev.
