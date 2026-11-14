# **ADR 0004: Kafka to Iceberg with Flink**

- Status: accepted
- Date: 2026-10-29

## **Context**
Phase 3 moves events from Kafka into the lakehouse. The goals are exactly-once results, safe restarts and upgrades, and independent evolution of producers and the table.

## **Decisions**
1. **Flink 2.1 on the Flink Kubernetes Operator** (`FlinkDeployment`). The operator owns lifecycle: it takes a savepoint on stop and restores from it on start, so upgrades are a spec change (`upgradeMode: savepoint`). The runtime is Java 17 (`flink:2.1.3-java17`), Flink's default and recommended Java version; Java 21 is only experimental in Flink 2.x and Java 25 is not supported. (Changed 2026-11-12: the job first ran on the `java21` image; the jar is now built for 17 to match.)
2. **DataStream API and Iceberg's `IcebergSink`.** The sink writes Parquet data files, and a committer creates the Iceberg snapshot only when the checkpoint covering those files completes (two-phase commit; re-commit after failure is idempotent). Together with Kafka offsets stored in checkpoints this gives exactly-once into the table. Freshness equals the checkpoint interval (10 s locally).
3. **De-duplicate on `event_id` in Flink state** (`keyBy(event_id)` with a 60-minute TTL). The ingestion service is at-least-once, and Flink's guarantee does not cover duplicates already in the input. State is part of every checkpoint and savepoint, so de-duplication survives restarts.
4. **Own Avro deserializer instead of the Confluent client libraries.** It reads Confluent-format records using the *writer* schema fetched from the registry by id, then maps fields to table columns by name (missing becomes null). This avoids the Confluent Maven repository and makes the evolution rules explicit: producers can add optional fields at any time without breaking the job.
5. **The job reads the table schema at startup.** A new column is a metadata-only `ALTER` followed by a savepoint restart. Rows written before then read back with null.
6. **Heap (hash map) state backend locally.** Simple and fast for the 60-minute id set. RocksDB is the choice if state outgrows memory (on AWS, see Consequences).
7. **Poison records are counted and skipped**, not allowed to crash-loop the job. The `deserializationErrors` metric exposes them; a dead-letter topic is a follow-up. A schema registry that cannot answer (unreachable, timeout, HTTP 5xx or 429) is not a poison record: the task fails and restarts from the last checkpoint, because skipping would commit the offset of a good event (changed 2026-11-12, after a code review).

## **Consequences**
- The state holds one boolean per event id for the TTL: roughly 100 bytes per id on heap. At sustained high rates this needs RocksDB or a shorter TTL. Size it before AWS. The hash-map backend also takes **full** snapshots; with 3-second checkpoints they exhausted the local S3 stand-in (see docs/spark-iceberg.md), so incremental RocksDB checkpoints are the intended change.
- The job jar is about 185 MB because it bundles the AWS SDK and Hadoop client. Acceptable for now; it can be slimmed by trimming the Hadoop dependency.
- Small files accumulate (one set per checkpoint per partition). Compaction is Phase 4.
- Locally the catalog is the Iceberg REST fixture on Postgres; AWS uses Glue. Only catalog properties change.
- Flink Kubernetes HA is required, not optional: without it, recovering a job that is not running falls back to the last savepoint, which replays and duplicates everything after it. This happened in Phase 5 (500,000 duplicate rows); HA is now on in every environment (see gitops/README.md).
