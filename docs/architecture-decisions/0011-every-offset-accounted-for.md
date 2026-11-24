# **ADR 0011: Every Kafka offset is accounted for**

- Status: accepted
- Date: 2026-11-19

## **Context**
A reviewer asked how the platform correlates what the API accepts with what reaches the lakehouse, and whether a record can be lost without anyone noticing. Before this change:

- The answer lived in the test harness: `eo_test verify-range` checks every id a test sent against the table. Nothing did that for production traffic.
- Flink counted and dropped records it could not decode. The `deserializationErrors` metric existed, but no alert watched it and the record was gone.
- Nothing in a row said where it came from in Kafka.

The API answers 200 only after every in-sync replica has the event. "Accepted by the API" therefore means "has a Kafka offset". Before Kafka, delivery is the source's job: no 200 means "not stored yet, retry with the same `event_id`" (ingestion README, Retry contract).

## **Decisions**
1. **Every row of `events` carries its Kafka partition, offset and timestamp** (`kafka_partition`, `kafka_offset`, `kafka_timestamp`). The job adds the columns at start-up with a metadata-only schema change; older rows read back null. The partition and offset are the join key between Kafka and the table.
2. **Every record the job reads but does not store goes to `events_skipped`** with its offset and a reason:
   - `duplicate`: a copy of an `event_id` already seen.
   - `undecodable`: not Avro, an unknown schema id, or a required column missing. The row also keeps the error and the raw bytes.

   It is a second `IcebergSink` that commits on the same checkpoints as `events`. Each offset therefore lands in exactly one of the two tables, with the same exactly-once guarantee.
3. **The dead-letter destination is a table, not the `events.dlq` topic.** A Kafka dead-letter write from Flink is exactly-once only with a transactional Kafka sink (transactions, a transactional id and Write ACLs for the Flink identity). It would also keep records only for the topic's retention, and checking it would mean reading Kafka. The table commits atomically with `events`, can be queried with SQL (Spark, Athena), and keeps records until someone deletes them.
4. **An hourly reconciliation checks offsets, not counts** (`events-reconcile`, Spark). The ingestion producer is idempotent and not transactional, so each partition's offsets are dense. For the last 24 hours (minus 15 minutes for commits in flight), every offset between a partition's first and last must appear exactly once across `events` and `events_skipped`.
   - The range is selected by Kafka timestamp but checked by offset. Several ingestion pods write to one partition and their timestamps are not in offset order, so cutting by timestamp would report holes that are not losses.
   - Results go to `events_reconciliation`: one row per run with totals, the missing offset ranges, and per-hour, per-partition and per-source counts.
5. **Alerts come from the run's snapshot summary.** The job writes its totals as `lakehouse.*` properties of the snapshot it commits; the iceberg-exporter publishes them as `iceberg_table_snapshot_property`. The alerts:
   - `LakehouseRecordsMissing` (critical): any missing offset.
   - `LakehouseRecordsDuplicated` (critical): an offset stored twice.
   - `LakehouseReconciliationStale` (warning): three missed runs. Its interval comes from the run itself, so a one-off run (interval 0) never fires it.
   - `FlinkRecordsUndecodable` (warning): `deserializationErrors` increased.

## **Consequences**
- **What a missing offset means.** Kafka has the record and the lakehouse does not. While it is within the topic's 7-day retention it can be replayed with `local/scripts/replay.sh`, which also re-sends undecodable records once they decode ([**ADR 0012**](0012-replay-through-the-api.md), added the same day). Nothing replays automatically.
- **The check depends on dense offsets.** It needs a non-transactional producer and no compaction. A transactional producer would put commit markers on offsets, and the check would have to read Kafka's own offsets instead.
- **What it cannot see.**
  - A loss at the very end of a partition (no later offset yet) shows up only once a later record arrives.
  - A loss of the lowest offset the tables hold for a partition in the window is not found (seen on EKS, 2026-11-24, at offset 0 of a fresh topic): the range starts at the lowest offset found. Closing it needs Kafka's earliest offset or the previous run's last one.
  - A record lost before Kafka acknowledged it has no offset; the source still holds it, because it never got a 200.
- **Event-level duplicates outside the 60-minute de-duplication window** are counted (`duplicate_event_ids`) but not alerted on: they are a documented limit of the retry contract, not a pipeline fault.
- **Cost.** `events_skipped` grows with retries (one row per duplicate copy) and with bad records. The reconciliation scans two columns of 24 hours of data every hour.
- **Recreating the topic restarts offsets at 0.** `reset_pipeline` (local tests) therefore drops `events_skipped` and `events_reconciliation` together with `events`.
- **Verified locally** by `make local-test-integrity` ([**results**](../integrity/README.md)). Not verified on EKS with Glue.
