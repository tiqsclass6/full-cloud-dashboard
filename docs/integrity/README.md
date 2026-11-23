# **Data integrity: every Kafka offset accounted for**

How the platform shows that what the API accepted is what reached the lakehouse, and what happens to records it cannot store. Design: [**ADR 0011**](../architecture-decisions/0011-every-offset-accounted-for.md). Alerts: [**pipeline runbook**](../runbooks/pipeline.md#flinkrecordsundecodable), [**lakehouse runbook**](../runbooks/lakehouse.md#lakehouserecordsmissing). Dashboard: **Lakehouse / Integrity** in Grafana (the last run's totals, replays, and what Flink set aside).

## **Where a record can fail, and what happens**
| **Stage**                         | **Failure**                                      | **What happens**                                                                                            |
| --------------------------------- | ------------------------------------------------ | ----------------------------------------------------------------------------------------------------------- |
| **Before the API**                | Network, source crash                            | The platform never saw it; the source keeps it until it gets a 200                                          |
| **API validation**                | Bad request                                      | 400 with the reason; nothing stored, the client keeps the event                                             |
| **API to Kafka**                  | Brokers slow or down                             | 503 or no reply; the client retries with the same `event_id`                                                |
| **Kafka**                         | Broker loss                                      | 200 only after all in-sync replicas (min 2 of 3) have it; kept 7 days                                       |
| **Flink: decode**                 | Not Avro, unknown schema, required column absent | Stored in `events_skipped` (`undecodable`) with the error and bytes; `FlinkRecordsUndecodable` fires        |
| **Flink: decode**                 | Schema registry unreachable                      | The job restarts from its checkpoint and reads the record again                                             |
| **Flink: de-duplication**         | A retried copy                                   | Stored in `events_skipped` (`duplicate`)                                                                    |
| **Flink crash, node loss**        | Anything in flight                               | Restart from the last checkpoint; uncommitted files are discarded and re-written                            |
| **Anything after Kafka, unknown** | A row lost or written twice                      | The hourly reconciliation finds the offset; `LakehouseRecordsMissing` or `LakehouseRecordsDuplicated` fires |

The correlation key is the Kafka partition and offset. The API answers 200 only once Kafka has assigned one, and every row in `events` or `events_skipped` carries it. The producer is idempotent and not transactional, so a partition's offsets have no holes. A hole in the lakehouse is therefore a record that was acknowledged and then lost.

## **Drill: `make local-test-integrity`**
One run on kind, started 2026-11-20 01:32 UTC ([**raw result**](results/drill.json)). Steps:

1. Send 3,000 events through the API, each 10th one twice (300 retried copies).
2. Write 3 non-Avro records straight to `events.raw` with the `kafka-admin` break-glass identity, standing in for a client that bypasses the API.
3. Reconcile the drill's window.
4. Delete one stored row with pyiceberg (the "silent loss"), then reconcile again.
5. Put the row back, then reconcile again.
6. Replay ([**ADR 0012**](../architecture-decisions/0012-replay-through-the-api.md)):
   - Dry-run the replay of the 3 undecodable records.
   - Delete a row older than Flink's de-duplication window (75 minutes) and reconcile its window.
   - Dry-run its replay, apply it, and check the table.
   - Dry-run it again, then reconcile again.

| **Check**                             | **Result**                                                                                   |
| ------------------------------------- | -------------------------------------------------------------------------------------------- |
| **Events in `events`**                | 3,000 of 3,000, no duplicates, each with its own partition and offset (partitions 0, 1, 3-5) |
| **Retried copies**                    | 300 rows in `events_skipped`, reason `duplicate`                                             |
| **Non-Avro records**                  | 3 rows, reason `undecodable`, partition 1, offsets 191259-191261, with error and bytes       |
| **`deserializationErrors`**           | 3 to 6 (3 left from an earlier attempt); `FlinkRecordsUndecodable` firing                    |
| **Reconcile before the loss**         | 3,303 offsets checked (3,000 + 300 + 3), 0 missing, 0 stored twice                           |
| **Reconcile after deleting one row**  | 1 missing, reported as `p3:277704-277704`, the deleted row's offset                          |
| **`LakehouseRecordsMissing`**         | Firing after the run; inactive again after the restore and the next run                      |
| **Reconcile after restoring the row** | 3,303 checked, 0 missing                                                                     |
| **Replay of the undecodable records** | 3 of 3 `UNDECODABLE` (still not Avro), 0 sent                                                |
| **Replay of a lost row**              | `p5:200590` deleted and reported missing; dry run `REPLAY`; applied: 1 sent, 0 failed        |
| **The replayed event**                | Stored once, at `p5:200744`, with `lakehouse.replayed_from = 5:200590:…`                     |
| **A second replay**                   | `ALREADY_REPLAYED`, nothing sent                                                             |
| **Reconcile after the replay**        | 3,303 checked, 0 missing, 1 replayed (2,999 stored + 300 + 3 + 1)                            |

## **How long the reconciliation takes**
| **Table state (`events`, 1.25 M rows)** | **Reconcile, drill window** |
| --------------------------------------- | --------------------------- |
| **1,630 uncompacted files**             | 516-638 s (3 runs)          |
| **6 files after compaction**            | 204 s (1 run)               |

Locally the compaction schedule is suspended, so the table had 1,630 files of about 20 KiB. Files written before the lineage columns existed have no statistics for them, so Iceberg cannot skip them. Compaction rewrites them with the columns. The runs used two executors with one core and 1 GiB each, on a laptop with about 17 of 19 GiB in use.

With the replay changes (2026-11-20, same window of the drill, one run each, on a cluster under heavy load):

| **Version**                                 | **Table scans** | **Snapshots read** | **Reconcile** |
| ------------------------------------------- | --------------- | ------------------ | ------------- |
| **`main` (before this change)**             | 4               | 2 per table        | 370 s         |
| **Replays, self-join, cached**              | 23              | 2 per table        | 339 s         |
| **Replays, window functions, cached**       | 6               | 2 per table        | 501 s         |
| **Replays, window functions, checkpointed** | 3               | 1 per table        | 364 s         |

The spread between single runs on a loaded cluster is larger than any difference between the versions, so these runs show no speed change. What they do show is the scan count, and that only the last version reads one snapshot of each table.

On 2026-11-23, after a fresh start of the nodes and with Langfuse off, the last version took **35 s** on the same window (one run; 73 s including pod start-up). The first drill on 2026-11-20 had also reconciled in 36-41 s, with Langfuse running. The 339-501 s runs therefore came from the state of the cluster after hours of drills, image loads and a laptop standby, not from the code. In the slow runs, every step was slow: 31 s to load the catalog client, 55 s to read 11 MB ([**memory notes**](../lab/RUNBOOK.md#memory)).

## **What went wrong**
- **The first reconciliation took 1,194 s.** It filtered by offset with a join, which Iceberg cannot push into the scan, and it evaluated the table scan three times. It now reads each table once, filtered by Kafka timestamp with a 1-hour slack, keeps that in memory and on disk, and checks offsets from it. The slack is the limit of the check: a record whose timestamp is more than an hour out of offset order is reported as missing (a unit test pins this).
- **The first drill stopped at the delete.** pyiceberg needs `pyiceberg-core` to rewrite a file in a partitioned table. The drill now installs it (0.10.1).
- **The reconciliation could read two versions of a table in one run** (in `main` since the reconciliation was added; found 2026-11-20). The scan was cached with `persist()`, but Spark re-resolved the tables for queries derived from it and missed the cache. Flink commits all the time, so the offset ranges and the counts could come from different snapshots. Rows are only added, so the totals could disagree with each other, but a lost row would still show. The window is now materialised once with a local checkpoint; the Iceberg scan reports show one snapshot per table.
- **Replay drill, first attempts.**
  - The replay pod failed: the Flink classes are not in the job's jar, so the pod now adds Flink's `lib/` to the classpath.
  - The runner then printed nothing, because a `grep` with no match ended the script (`set -e`). It now prints the pod's log when the pod fails.
  - One drill ran against the old Spark image (not rebuilt), so the reconcile did not count the replay and the drill failed. The recorded run is the next one.
- **The recorded run took 3 h 40 min instead of about 40 minutes.** The laptop went into standby on idle, which froze one reconcile for 3 h 16 min. It finished correctly after the wake-up. Long drills need standby off.

## **What this does not show**
- EKS with the Glue catalog, and a table the size of a real day.
- Replaying an undecodable record that decodes once its schema is fixed. The drill's records are not Avro, so they stay undecodable; the decision rules for that case are unit tests (`ReplayTest`).
- `WAIT_FOR_DEDUPE_TTL`, `NOT_IN_KAFKA`, `EVENT_ID_STORED` and `NOT_SENDABLE` on a cluster: unit tests only. The replay of more than one record, and replays on EKS.
- A loss at the very end of a partition. It is reported only once a later offset in that partition arrives.
- The scheduled job. Locally it is suspended like the other Spark schedules; the drill ran it as one-off runs, which disarm the staleness alert. `LakehouseReconciliationStale` is covered by promtool tests only.
- The ops agent's handling of the new alerts (live model runs are blocked, see [**ai-ops.md**](../ai-ops.md#not-verified)).
- Single runs.
