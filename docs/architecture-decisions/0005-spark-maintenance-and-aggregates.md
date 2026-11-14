# **ADR 0005: Spark for table maintenance and aggregates**

- Status: accepted
- Date: 2026-10-29

## **Context**
The streaming pipeline leaves a small-file problem behind (a few files per checkpoint, plus snapshots and manifests), and the platform needs derived datasets. Both are batch work.

## **Decisions**
1. **PySpark on Kubernetes through the Kubeflow Spark operator** (`SparkApplication`, `ScheduledSparkApplication`). The operator submits and tracks jobs, retries failures and handles schedules, so there is no hand-written spark-submit orchestration. The Apache-owned operator was considered; the Kubeflow one was chosen for its scheduled applications and wider use.
2. **Spark 4.1 with Iceberg 1.12 and the Iceberg runtime baked into the image**, so job pods start without downloading dependencies. The same Iceberg version as the Flink job.
3. **Maintenance through Iceberg's stored procedures** (`rewrite_data_files`, `rewrite_manifests`, `expire_snapshots`, `remove_orphan_files`) rather than custom rewrite code. They are the maintained, concurrency-aware implementation; the job adds ordering, safe defaults, statistics and a summary.
4. **Compaction runs while the stream is writing.** Iceberg's optimistic concurrency lets a rewrite of existing files and a concurrent append both commit, so no pause of the Flink job is needed. This is verified by a test (see the results page).
5. **Conservative orphan and expiry defaults** (72 hours for orphans; expiry keeps at least 10 snapshots), because unreferenced files younger than the longest checkpoint-plus-recovery time can belong to in-flight writes.
6. **Aggregates are idempotent `MERGE`s over hour-aligned windows with a look-back**, not append-only jobs. Re-runs, backfills and late data all converge to the same result.
7. **Tests run inside the production Spark image.** Fast enough, realistic (Linux, Java 21, the same jars), and independent of the Windows host's Hadoop limitations.
8. **Schedules are defined now and applied suspended locally**; they run unsuspended on AWS in Phase 6.

## **Consequences**
- Table health becomes an operational concern with its own schedule and its own failure modes. Phase 8 adds metrics and alerts for it (file counts, small-file ratio, last compaction age).
- The local S3 stand-in (SeaweedFS) is a weak point under combined load: it needed a larger memory limit and a higher volume cap, and it reclaims deleted data only when vacuumed. Real S3 does not have these limits.
- Full-state checkpoints of a large de-duplication state, taken very often, are expensive. See the Flink job notes: RocksDB with incremental checkpoints is the likely change before AWS.
