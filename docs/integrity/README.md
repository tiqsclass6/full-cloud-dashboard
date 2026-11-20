# **Data integrity: every Kafka offset accounted for**

How the platform shows that what the API accepted is what reached the lakehouse, and what happens to records it cannot store. Design: [**ADR 0011**](../architecture-decisions/0011-every-offset-accounted-for.md). Alerts: [**pipeline runbook**](../runbooks/pipeline.md#flinkrecordsundecodable), [**lakehouse runbook**](../runbooks/lakehouse.md#lakehouserecordsmissing).

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
One run on kind, 2026-11-19 ([**raw result**](results/drill.json)). Steps:

1. Send 3,000 events through the API, each 10th one twice (300 retried copies).
2. Write 3 non-Avro records straight to `events.raw` with the `kafka-admin` break-glass identity, standing in for a client that bypasses the API.
3. Reconcile the drill's window.
4. Delete one stored row with pyiceberg (the "silent loss"), then reconcile again.
5. Put the row back, then reconcile again.

| **Check**                             | **Result**                                                                                   |
| ------------------------------------- | -------------------------------------------------------------------------------------------- |
| **Events in `events`**                | 3,000 of 3,000, no duplicates, each with its own partition and offset (partitions 0, 1, 3-5) |
| **Retried copies**                    | 300 rows in `events_skipped`, reason `duplicate`                                             |
| **Non-Avro records**                  | 3 rows, reason `undecodable`, partition 1, offsets 190353-190355, with error and bytes       |
| **`deserializationErrors`**           | 0 to 3; `FlinkRecordsUndecodable` firing                                                     |
| **Reconcile before the loss**         | 3,303 offsets checked (3,000 + 300 + 3), 0 missing, 0 stored twice                           |
| **Reconcile after deleting one row**  | 1 missing, reported as `p3:276204-276204`, the deleted row's offset                          |
| **`LakehouseRecordsMissing`**         | Firing after the run; inactive again after the restore and the next run                      |
| **Reconcile after restoring the row** | 3,303 checked, 0 missing                                                                     |

## **How long the reconciliation takes**
| **Table state (`events`, 1.25 M rows)** | **Reconcile, drill window** |
| --------------------------------------- | --------------------------- |
| **1,630 uncompacted files**             | 516-638 s (3 runs)          |
| **6 files after compaction**            | 204 s (1 run)               |

Locally the compaction schedule is suspended, so the table had 1,630 files of about 20 KiB. Files written before the lineage columns existed have no statistics for them, so Iceberg cannot skip them. Compaction rewrites them with the columns. The runs used two executors with one core and 1 GiB each, on a laptop with about 17 of 19 GiB in use.

## **What went wrong**
- **The first reconciliation took 1,194 s.** It filtered by offset with a join, which Iceberg cannot push into the scan, and it evaluated the table scan three times. It now reads each table once, filtered by Kafka timestamp with a 1-hour slack, keeps that in memory and on disk, and checks offsets from it. The slack is the limit of the check: a record whose timestamp is more than an hour out of offset order is reported as missing (a unit test pins this).
- **The first drill stopped at the delete.** pyiceberg needs `pyiceberg-core` to rewrite a file in a partitioned table. The drill now installs it (0.10.1).

## **What this does not show**
- EKS with the Glue catalog, and a table the size of a real day.
- Recovery of a missing or undecodable record: it is a manual runbook step, and no tool replays records.
- A loss at the very end of a partition. It is reported only once a later offset in that partition arrives.
- The scheduled job. Locally it is suspended like the other Spark schedules; the drill ran it as one-off runs, which disarm the staleness alert. `LakehouseReconciliationStale` is covered by promtool tests only.
- The ops agent's handling of the new alerts (live model runs are blocked, see [**ai-ops.md**](../ai-ops.md#not-verified)).
- Single runs.
