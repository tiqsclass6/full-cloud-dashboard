# **ADR 0013: The reconciliation takes its offset ranges from Kafka**

- Status: accepted
- Date: 2026-11-24

## **Context**
[**ADR 0011**](0011-every-offset-accounted-for.md) checks, per partition, every offset between the first and the last one in the window. Both ends came from the tables: the lowest and highest offset found in `events` and `events_skipped`. A hole between two rows was found, but a lost row at either end left no hole:

- **The lowest offset.** On EKS (2026-11-24) the oldest row was offset 0 of a fresh topic. Deleted, it was not reported: the check covered 5,502 offsets, not 5,503.
- **The highest offset.** Reported only once a later record in that partition arrived (a known limit).
- **A whole partition.** If no row of a partition was in the window, the partition was not checked at all.

The tables cannot say what they are missing. Kafka can: it indexes every offset by timestamp.

## **Decisions**
1. **The range per partition comes from Kafka's offset index.** From the first offset at or after the window start (`OffsetSpec.forTimestamp(start)`) to the last one before the first offset at or after the window end. If no record is that new, the partition's end offset is used.
2. **The upper end is capped at Flink's committed offset** (group `flink-events-to-iceberg`). Flink commits it on a checkpoint, together with the Iceberg commit. Records past it are late, not lost: a Flink outage stays a lag alert, not a `LakehouseRecordsMissing`. Runs report how many partitions were capped (`partitions_behind_flink`).
3. **The lookup happens before the tables are read.** A Flink commit between the two then only adds rows; it never makes the scan miss an offset the lookup expects.
4. **Gaps at both edges count.** Each range gets an edge just outside it on both sides, so a missing first or last offset, or a range with no row, shows in `missing` and `missing_ranges`. A missing tail is counted in the window end's hour.
5. **Its own Kafka identity.** The Spark driver uses the Kafka Java client (`kafka-clients`, the ingestion service's version, in the Spark image) through Spark's JVM, as the KafkaUser `lakehouse-reconcile`: `Describe` on `events.raw` and on Flink's group. It can list offsets and read the group's committed offsets, but not read or write records. Only the reconcile schedule's driver mounts the certificate; executors never talk to Kafka.

## **Consequences**
- **The reconciliation needs Kafka.** If the brokers or the certificate are unavailable, the run fails, and `LakehouseReconciliationStale` fires after three missed runs. There is no fallback to table-derived ranges: that would silently bring back the gaps.
- **Late records are not losses until Flink commits past them.** A record lost by Flink after its committed offset moved on is found; one in a batch Flink has not committed yet is checked by a later run.
- **A one-off run with `--settle-minutes 0` can race a checkpoint.** Flink commits the Kafka offset and the Iceberg snapshot on the same checkpoint, but not atomically together. Read in the moment between the two, the committed offset can be one checkpoint ahead of the table. The schedule settles 15 minutes, far longer than a checkpoint (10 s), and the window end then decides. The drills wait for two checkpoints after the consumer lag reaches 0.
- **What is still out of reach.** A record lost before Kafka acknowledged it has no offset (the source keeps it: no 200). Offsets must stay dense (ADR 0011).
- **The test for the lookup itself** runs against a broker container (`apps/spark-jobs/run-kafka-it.sh`); the unit tests pass bounds in directly.
