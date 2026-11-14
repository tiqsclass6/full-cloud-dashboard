---
title: Flink to Iceberg Observability
layout: default
---

## **Observability**

Kafka to Iceberg with Flink: test results

The job ([**apps/flink-iceberg-job**](../apps/flink-iceberg-job/README.md)) reads `events.raw` and writes the Iceberg table `lakehouse.events`. This page records what was tested on the local kind cluster and how to repeat it. Design reasons are in [**ADR 0004**](architecture-decisions/0004-flink-to-iceberg-pipeline.md).

## **Exactly-once test**

`make local-test-exactly-once` (**destructive**: it deletes the `events.raw` topic, the consumer group and the table first).

The test sends events with deterministic ids, so it knows the exact set that must end up in the table, once each. Every wave also sends each tenth event a second time, like a client retrying after a timeout.

| **Step**    | **What happens**                                                                                               | **Table after (rows / distinct ids / duplicates)**     |
| ----------- | -------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------ |
| **Wave 1**  | 8,000 events + 800 duplicate sends                                                                             | 8,000 / 8,000 / 0                                      |
| **Wave 2**  | 8,000 events + 800 duplicate sends; the TaskManager pod is deleted 3 s into the wave                           | 16,000 / 16,000 / 0                                    |
| **Wave 3**  | 8,000 events + 800 duplicate sends; the job is stopped with a savepoint and restored from it 3 s into the wave | 24,000 (see below)                                     |
| **Re-send** | 1,000 events that were delivered before the restart are sent again                                             | **24,000 / 24,000 / 0**, none missing, none unexpected |

Result: **passed**. 2,400 duplicate sends and 1,000 post-restart re-sends were all dropped, and nothing was lost across a pod kill and a savepoint stop/restore. The last point matters: the re-sent events were recognised after the restart, which shows the de-duplication state came back from the savepoint.

### **What this does and does not prove**

- The producer sends a wave in about 3 seconds, so the faults came after the events were already in Kafka. Most likely Flink had read some of them but not yet committed them (commits happen on the 10-second checkpoint), which is the hard case, but the run did not record exactly where each fault fell relative to a checkpoint. It is also not a test of faults during a long, steady stream; a longer-running variant would be stronger.
- One run of each fault. It shows the mechanism works, not that no rare interleaving exists.
- Kafka, the catalog and S3 were not failed. Broker failure, latency and a node outage are covered by the [**chaos experiments**](chaos/README.md).
- Verification reads the table through PyIceberg (a different client than the writer), so it checks what readers actually see.

## **Operational notes from the run**

- After the TaskManager pod was deleted the job came back on its own and the table ended up correct.
- Stopping with a savepoint and restoring took about 80 seconds from "suspended" to "running again" (this includes the time the wave was still being produced). The time was not broken down further.
- The table gained one Iceberg snapshot and two data files per wave (4 / 6 / 8 files after waves 1 / 2 / 3). With a 10-second checkpoint interval and sustained traffic this means many small files, which compaction (Phase 4) will merge.
- Each TaskManager logged `Name collision: ... 'pendingCommittables'` on every checkpoint (Flink bug [**FLINK-35321**](https://issues.apache.org/jira/browse/FLINK-35321): the sink committer re-registers its gauge). The fix is on Flink's master branch but in no release yet (latest 2.3.0), so the chart's log configuration (`helm/charts/flink-jobs/files/log4j-console.properties`) drops that one message; other name collisions are still logged. Remove the filter once the job runs a release with the fix.

## **Schema evolution demo**

`make local-demo-schema-evolution` (also destructive: it resets the topic and table). It adds an optional `region` field from the producer through to the table with no downtime and no data rewrite.

| **Step** | **What happens**                                                                                                                                             |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **1**    | The ingestion service runs as a **schema v1 producer** (`SCHEMA_RESOURCE=/schemas/event-v1.avsc`). 500 events flow into the table                            |
| **2**    | `SchemaTool add-column region string` adds the column to the Iceberg table. This is a metadata-only change; the running job is untouched and stays `RUNNING` |
| **3**    | The job is stopped with a savepoint and restored. On start it reads the table schema and now fills `region`                                                  |
| **4**    | The ingestion service switches to **schema v2** (the optional `region` field with a null default) and registers it with the registry                         |
| **5**    | 500 events with `region=eu-west-1` and 500 without it are sent through the v2 service                                                                        |
| **6**    | PyIceberg verifies the table                                                                                                                                 |

Result: **passed**.

```json
{"schema_columns": ["event_id","source","type","event_time","ingest_time","value","attributes","region"],
 "schema_versions": 2, "rows": 1500, "distinct_ids": 1500,
 "with_region": 500, "null_region": 1000, "oldest_snapshot_rows": 500}
```

- The table has the new column and two schema versions.
- All 500 rows written under schema v1 and all 500 v2 rows sent without a region read back with `region = null`, and the 500 with a region carry `eu-west-1`.
- The oldest snapshot (taken before the column existed) is still readable.
- No rows were lost or duplicated.

### **Caveats**

- The registry keeps its state across runs, so the "versions: [1,2]" line printed in step 4 is not a fresh registration on repeat runs. The compatibility behaviour is what the test checks.
- Only the recommended order was run end to end (table, then job, then producers). The reverse order (producer first) is covered by a unit test, where the job ignores a field the table does not have yet, but it was not run on the cluster. In that order values sent before the column exists are not stored.
- Only an added optional column was tested. Renames, type changes and drops are not handled by the job.

## **Bugs found while building this**

- `IcebergTables.ensureTable` closed the REST catalog before returning the table, so a later write through that table (the `ADD COLUMN`) failed with "Connection pool shut down". Reading the cached schema had worked, which hid it. It was found by the first evolution run and fixed by letting callers own the catalog.
- A demo run failed because a stale `kubectl port-forward` kept the old ingestion pod's port. The scripts now start the process directly so it can be killed.

## **Lesson from Phase 4: checkpoint cost and storage**

While generating data for the Spark jobs, the checkpoint interval was shortened to 3 seconds. With the hash-map state backend every checkpoint is a full snapshot of the de-duplication state (millions of event ids), so checkpoints wrote hundreds of MB each. The local S3 stand-in ran out of volume slots, S3 writes failed, and Flink stalled with a backlog. It resumed by itself once storage was fixed and the table then matched Kafka exactly, with no loss and no duplicates. The details are in [**docs/spark-iceberg.md**](spark-iceberg.md). Before AWS the state backend should move to RocksDB with incremental checkpoints, and the interval should stay at 10 seconds or more unless there is a reason.

## **Re-run under GitOps (Phase 5, 2026-10-29)**

After the move to Helm and Argo CD, both scripts were re-run on a freshly built cluster. They now reset through Argo CD: automated sync is paused, the job, topic and table are deleted, then sync resumes and Argo CD recreates the topic and the job from Git.

| **Test**                                                                             | **Result**                                                                  |
| ------------------------------------------------------------------------------------ | --------------------------------------------------------------------------- |
| **Exactly-once (3 waves, TaskManager kill, savepoint stop/restore, 1,000 re-sends)** | **Passed:** 24,000 rows, 24,000 distinct ids, none missing, none unexpected |
| **Schema evolution (v1 producer, add column, savepoint restart, v2 producer)**       | **Passed:** 1,500 rows, all distinct, 500 with `region`, 1,000 null         |

After each run Argo CD restored the deployments from Git; for example, the demo's temporary `SCHEMA_RESOURCE` override on the ingestion service was gone and automated sync was back on. One difference from the Phase 3 demo run: the table's oldest snapshot was an empty initial commit (0 rows) instead of the first 500 events. The demo does not assert on it, and it is still readable.

## **Re-run on Java 17 (2026-11-12)**

The job moved from `flink:2.1.3-java21` to `flink:2.1.3-java17` (ADR 0004), and every image was rebuilt from scratch for `linux/amd64` (`--pull --no-cache`). Both tests were then run once, on kind, against the new images.

| **Test**                                          | **Result**                                                                                |
| ------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| **Exactly-once** (`make local-test-exactly-once`) | **Passed:** 24,000 rows, 24,000 distinct ids, none missing, none unexpected, 0 duplicates |
| **Recovery** (`make local-test-recovery`)         | **Passed:** +20,000 rows for +20,000 Kafka messages; 64,000 rows, all distinct            |

On 2026-11-13 the exactly-once test passed again (24,000 / 24,000 / 0), and so did the schema evolution demo (1,500 rows, all distinct, 500 with `region`, 1,000 null, 2 registry versions). Both ran while `make local-ports` was running and reused its catalog and S3 forwards.

In the recovery run, the operator restarted the FAILED job from the latest checkpoint (checkpoint 32), not from a savepoint. This is a single run on one laptop cluster. It does not show behaviour under sustained load or on EKS, and it does not exercise the registry-outage path (`RegistryUnavailableException`), which only the unit tests cover.
