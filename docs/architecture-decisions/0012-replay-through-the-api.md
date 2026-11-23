# **ADR 0012: Lost and undecodable records are replayed through the ingestion API**

- Status: accepted
- Date: 2026-11-19

## **Context**
[**ADR 0011**](0011-every-offset-accounted-for.md) made every Kafka offset visible: a record is in `events`, in `events_skipped`, or reported missing by the reconciliation. Recovery was still manual. The runbooks said to read the offsets back, decode them and send them again by hand. No tool did it, and nothing replayed undecodable records once their schema was fixed.

A replay has to keep the guarantees the pipeline already has:

- Each event is stored once, under its original `event_id`.
- Only the ingestion service writes to `events.raw` (Kafka ACLs).
- The reconciliation can tell that the lost offset is accounted for.

## **Decisions**
1. **A command-line tool in the Flink job's jar** (`io.lakehouse.flink.ReplayTool`, run by `local/scripts/replay.sh`) with two modes:
   - `missing --partition P --offset O [--count N]`: offsets the reconciliation reported missing, read back from Kafka.
   - `undecodable [--since T | --partition P --offset O]`: the `payload` of `events_skipped` rows with reason `undecodable`, decoded with the registry as it is now.

   It reuses the job's decoder and configuration, so it decodes exactly as the job does. It runs as a one-off pod copied from the JobManager: the same image, environment, service account (IRSA on AWS) and Kafka client certificate. The tool gets no access the job does not already have.
2. **It sends through the ingestion API, not to Kafka or Iceberg.** The Flink identity may only read `events.raw`. Writing straight to Iceberg would bypass the de-duplication and the lineage columns. Through the API, a replayed event is validated, gets a new offset, and goes through Flink like any other event. The tool follows the API's retry contract (retry 429, 503 and no reply with the same ids; stop on 400).
3. **The original `event_id` and a marker attribute.** The event keeps its id, source, type, event time, value, region and attributes. It also gets `lakehouse.replayed_from = <partition>:<offset>:<kafka timestamp ms>`. The reconciliation counts a marker as accounting for the original offset only when that offset has no row of its own. A replayed undecodable record already has its `events_skipped` row, so it is not counted twice. Runs report `replayed`.
4. **Dry run by default; `--apply` sends.** The tool prints one JSON line per offset with its outcome, and a summary. It never replays an offset that is stored, skipped or already replayed, or an event whose id is stored at another offset.
5. **It waits out Flink's de-duplication window for lost records.** A lost record was seen by Flink, which keeps its id for `DEDUPE_TTL_MINUTES` (60) and would drop a copy as a retry. The tool refuses (`WAIT_FOR_DEDUPE_TTL`) until the Kafka timestamp plus the TTL plus 10 minutes, and prints when to retry. Undecodable records are replayed at once: Flink never stored their ids.

## **Consequences**
- **What it cannot recover.** A record past Kafka's 7-day retention (`NOT_IN_KAFKA`), bytes that still do not decode (`UNDECODABLE`), a record with 32 attributes already (no room for the marker), or a field the API does not accept (`NOT_SENDABLE`). Only the source can resend these.
- **Recovery takes at least 70 minutes after the loss.** That is the price of not bypassing the de-duplication.
- **The replayed row is a new Kafka record.** It has its own partition, offset and Kafka timestamp. Its `ingest_time` is the replay time and its `event_time` is the original's. The hourly aggregate recomputes only the last 3 hours of event time, so an older replayed hour needs a backfill (`aggregate --start ... --end ...`).
- **The marker is visible to consumers** of `events.attributes`. It is the record of the repair, the way `events_skipped` is the record of what was set aside.
- **The checks read with plain Iceberg scans**, one per table, per run. On a large table the event-id check scans the whole `events` table. That is acceptable for a manual repair tool, but slow.
- **Verified locally** by step 4 of `make local-test-integrity` ([**results**](../integrity/README.md)). Not verified on EKS: the runner supports `KUBE_CONTEXT=lakehouse-<env>`, but it has not been run there.
