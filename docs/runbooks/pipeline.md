# **Runbook: streaming pipeline (Kafka to Flink to Iceberg)**

Alerts from `helm/charts/observability/rules/pipeline.yaml`. Dashboard: **Lakehouse / Pipeline**. Commands assume `source local/scripts/lib.sh` (local) or `KUBE_CONTEXT=lakehouse-dev` (AWS).

## **IcebergTableStale**

**Meaning:** no commit to `lakehouse.events` for over 5 minutes. The Flink sink commits on every checkpoint (every 10 s), even with no new data, so a stale table means checkpoints are not completing or the job is down.
**Impact:** queries (Athena, Spark) see no new data; the freshness SLO is being spent.
**Diagnose:**

1. Is the job running? `kc -n apps get flinkdeployment events-to-iceberg -o jsonpath='{.status.jobStatus.state}'` (see FlinkJobNotRunning).
2. Are checkpoints completing? Pipeline dashboard, "Checkpoints" panel (see FlinkCheckpointsStalled).
3. Can the job reach the catalog and storage? In Loki: `{namespace="apps", app="events-to-iceberg"} |= "Exception"`.
**Fix:** follow the alert that explains the cause. Data is not lost while the table is stale: events wait in Kafka and are committed once checkpoints resume.

## **FlinkJobNotRunning**

**Meaning:** no running Flink job reports uptime for 5 minutes.
**Impact:** events accumulate in `events.raw` (7-day retention); nothing reaches Iceberg.
**Diagnose:** `kc -n apps describe flinkdeployment events-to-iceberg` (status and events); JobManager logs in Loki: `{namespace="apps", app="events-to-iceberg", container="flink-main-container"}`. Common causes: catalog or S3 unreachable at start-up, an expired Kafka certificate copy, a Kyverno or Pod Security rejection after a spec change (`kc -n apps get events`).
**Fix:** the operator restarts a FAILED job automatically (`restartFailedJob`, with HA, so it resumes from the latest checkpoint). If it stays down, fix the cause and force a restart: `kc -n apps patch flinkdeployment events-to-iceberg --type merge -p '{"spec":{"restartNonce":'$(date +%s)'}}'`. Never restart from an old savepoint (docs/flink-iceberg.md, the 500,000-duplicate incident).

## **FlinkCheckpointsStalled**

**Meaning:** no checkpoint completed for 10 minutes while the job is up.
**Impact:** nothing new becomes visible in Iceberg; on recovery the job replays from the last completed checkpoint.
**Diagnose:** Pipeline dashboard: checkpoint duration and failures, backpressure. Logs: `|= "checkpoint"`. Usual causes: S3 or the HA store unreachable (network policy, credentials), a slow Iceberg commit (catalog), extreme backpressure.
**Fix:** restore access to storage or the catalog; if backpressure, see FlinkBackpressureHigh.

## **FlinkCheckpointFailures**

**Meaning:** at least one checkpoint failed in 10 minutes. The job tolerates 3 consecutive failures before restarting.
**Impact:** if failures keep happening, the job will eventually restart, potentially delaying data visibility in Iceberg.
**Diagnose and fix:** as FlinkCheckpointsStalled. A single failure during a broker restart or node drain is expected.

## **FlinkJobRestarting**

**Meaning:** more than 2 restarts in 15 minutes.
**Impact:** each restart replays from the last checkpoint (no duplicates thanks to exactly-once, but freshness suffers).
**Diagnose:** the root exception in the JobManager log (`|= "switched from RUNNING to RESTARTING"`, then the `Caused by` lines). `Heartbeat of TaskManager ... timed out` means a TaskManager did not answer for `heartbeat.timeout` (20 s): its node failed, or it stalled (GC pause, memory pressure). Several of these without a node failure suggest raising `flink-jobs.heartbeat.timeout`.
**Fix:** depends on the cause; a poison message that cannot be deserialised needs the schema history checked (docs/flink-iceberg.md).

## **FlinkBackpressureHigh**

**Meaning:** operators are back-pressured more than half the time over 10 minutes.
**Impact:** consumer lag grows; checkpoints take longer.
**Diagnose:** which operator is the bottleneck (Pipeline dashboard). Usually the Iceberg writer, limited by S3 latency or TaskManager CPU.
**Fix:** raise `flink-jobs.parallelism` (up to the 6 Kafka partitions) or TaskManager resources in Git; Argo CD applies it with a savepoint upgrade.

## **KafkaConsumerLagHigh**

**Meaning:** more than 50,000 events are waiting for the Flink job.
**Impact:** data reaches Iceberg late; if it keeps growing, freshness alerts follow.
**Diagnose:** is the job running and keeping up (records in vs out)? Did ingestion spike (Ingestion dashboard)? `kafka_cli kafka-consumer-groups.sh --describe --group flink-events-to-iceberg`.
**Fix:** as FlinkBackpressureHigh, or wait out a burst if lag is falling.
