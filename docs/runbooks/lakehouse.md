# **Runbook: Iceberg tables and Spark jobs**

Alerts from the `iceberg`, `integrity` and `platform` groups in `helm/charts/observability/rules/lakehouse.yaml`. Dashboards: **Lakehouse / Tables**; for the `integrity` group, **Lakehouse / Integrity**. Table health comes from the iceberg-exporter (`apps/iceberg-exporter`).

## **IcebergExporterDown**

**Meaning:** the exporter has not been able to load a table for 15 minutes.
**Impact:** the other Iceberg alerts are blind for that table.
**Diagnose:** `kc -n apps logs deploy/iceberg-exporter` (the failure is logged per table). A table that does not exist yet (`events_hourly` before the first aggregate run, `events_reconciliation` before the first reconcile run) also shows as down.
**Fix:** catalog or storage access (network policy, IRSA on AWS), or create the table by running the job that owns it.

## **IcebergSmallFilesHigh**

**Meaning:** over 1,000 data files below 32 MiB for 30 minutes.
**Impact:** every scan opens many small files: slower queries, higher S3 request cost, larger manifests.
**Fix:** check that the hourly compaction (`events-compaction`) is scheduled and succeeding (SparkJobFailed); run it once by hand: `bash local/scripts/spark-run.sh compact maintenance --steps compact,manifests`.

## **IcebergCompactionOverdue**

**Meaning:** no compaction for over 6 hours while small files keep accumulating.
**Impact:** query performance may degrade and S3 request costs increase.
**Fix:** as IcebergSmallFilesHigh. A table that was never compacted does not fire this alert (the timestamp is NaN).

## **IcebergSnapshotsAccumulating**

**Meaning:** over 2,000 snapshots in the table metadata for an hour.
**Impact:** the metadata file grows with every commit (every 10 s), and data files replaced by compaction are never deleted.
**Fix:** check the daily `events-expire` job; run it by hand: `bash local/scripts/spark-run.sh expire maintenance --steps expire,orphans`.

## **LakehouseRecordsMissing**

**Meaning:** the last run of the hourly `events-reconcile` job found Kafka offsets that are in neither `events` nor `events_skipped`. The ingestion producer is idempotent and not transactional, so offsets are dense: each hole is a record Kafka acknowledged (the API answered 200) that the lakehouse does not have. The offsets checked come from Kafka: per partition, from the window start to the window end, but not past Flink's committed offset, so a record Flink has not committed yet is not reported.
**Impact:** counts and aggregates built on `events` are short for the hours listed.
**Diagnose:** the run's details: `SELECT run_at, missing, missing_ranges FROM lakehouse.lakehouse.events_reconciliation ORDER BY run_at DESC LIMIT 1` (ranges read `p<partition>:<first>-<last>`); `details` gives the hour and partition. Then ask why those offsets were not committed: Flink restarts or restores from an old savepoint around that time (FlinkJobRestarting; a restore that skips ahead), a manual delete or rewrite of `events` (Iceberg `snapshots` and `history` metadata tables), or a topic that was recreated without resetting the tables.
**Fix:** fix the cause first, then replay each range while it is still in Kafka (7-day retention). For `p3:276204-276210`: `bash local/scripts/replay.sh missing --partition 3 --offset 276204 --count 7` prints one line per offset and what it would do; add `--apply` to send. The tool reads the offsets with the Flink identity (read-only on `events.raw`), decodes them, and sends them through the ingestion API with their original `event_id` and the attribute `lakehouse.replayed_from`, which the reconciliation counts for the original offset. It skips offsets already stored, skipped or replayed, and events whose id is stored elsewhere. It refuses (`WAIT_FOR_DEDUPE_TTL`) until 70 minutes after the record's Kafka timestamp, because before that Flink's de-duplication would drop the copy as a retry; it prints when to retry. `NOT_IN_KAFKA` means retention has passed: only the source can resend it. On EKS, set `KUBE_CONTEXT=lakehouse-dev`. Then run the job once (`bash local/scripts/spark-run.sh events-reconcile reconcile --settle-minutes 0`) to confirm 0; the alert clears with that run.

## **LakehouseRecordsDuplicated**

**Meaning:** the same partition and offset is stored more than once across `events` and `events_skipped`.
**Impact:** counts are inflated. Exactly-once between Kafka and Iceberg did not hold, or a row was copied in by hand.
**Diagnose:** find the offsets: `SELECT kafka_partition, kafka_offset, count(*) FROM lakehouse.lakehouse.events GROUP BY 1, 2 HAVING count(*) > 1`. Compare the snapshots that added them (`events.snapshots`, `events.files`): two Flink commits of the same checkpoint, a restore from an old savepoint, or a manual insert.
**Fix:** remove the extra copies (`make local-repair-duplicates` handles duplicate `event_id`s) and find the cause before the next run.

## **LakehouseReconciliationStale**

**Meaning:** the scheduled reconciliation has not committed a result for three of its intervals. A one-off run records an interval of 0 and never fires this.
**Impact:** a loss or a duplicate would go unnoticed.
**Diagnose:** `kc -n apps get scheduledsparkapplications events-reconcile` (suspended? last run?), SparkJobFailed, the driver log (`SUMMARY` line). The job reads Kafka's offsets first, with the KafkaUser `lakehouse-reconcile`: a Kafka error in the driver log points to the brokers or to that certificate (`kc -n apps get secret lakehouse-reconcile`, copied in by Kyverno).
**Fix:** as SparkJobFailed; run it once by hand to confirm.

## **SparkJobFailed**

**Meaning:** a Spark application failed in the last hour.
**Diagnose:** `kc -n apps get sparkapplications`; the driver log in Loki: `{namespace="apps", pod=~"<app>-driver"}`; `kc -n apps describe sparkapplication <app>` for submission errors. Admission rejections (Pod Security, Kyverno) appear in the application's error message.
**Fix:** depends on the cause; the schedule retries on its next run, and failed runs are kept (3) for inspection.
