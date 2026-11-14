# **Spark jobs on the events table: results**

What the PySpark jobs ([**apps/spark-jobs**](../apps/spark-jobs/README.md)) did on the local kind cluster, how it was measured, and what went wrong along the way. Design reasons are in [**ADR 0005**](architecture-decisions/0005-spark-maintenance-and-aggregates.md).

## **Setup**

- **Data:** the `lakehouse.events` table written by the Flink job: 2,501,500 rows to start, produced through the real path (ingestion service, Kafka, Flink, Iceberg). To get many small files quickly, the Flink checkpoint interval was shortened to 3 s for this data generation only (the normal setting is 10 s); see "What went wrong".
- **Spark:** 4.1.4 on Kubernetes through the Kubeflow Spark operator; one driver (1 core, 1 GiB) and two executors (1 core, 1 GiB each). Iceberg 1.12.0 through the REST catalog; S3 is the local SeaweedFS.
- **Everything shares one laptop** with Kafka, Flink and Prometheus, so absolute times are not meaningful. The comparisons inside each experiment are.

## **1. Compaction and maintenance**

The table before maintenance was in the state a streaming job leaves behind: 70 files averaging 1 MiB, 56 snapshots, 35 manifests.

`make local-spark-maintenance` (compact, manifests, expire; expiry used aggressive retention, "older than 0 hours, keep the last 1", to show the effect):

|                                                                 | **Before**            | **After**    |
| --------------------------------------------------------------- | --------------------- | ------------ |
| **Data files**                                                  | 70 (all under 32 MiB) | 1 (72.8 MiB) |
| **Average file size**                                           | 1.06 MiB              | 72.8 MiB     |
| **Snapshots**                                                   | 56                    | 1            |
| **Manifests**                                                   | 35                    | 1            |
| **Total size**                                                  | 74.1 MiB              | 72.8 MiB     |
| **Rows**                                                        | 2,501,500             | 2,501,500    |
| **Median scan time (aggregate over all rows, after a warm-up)** | 1.68 s                | 0.48 s       |

- Step cost: compaction 10.5 s (rewrote 77.7 MB), manifest rewrite 4.2 s, expiry 11.3 s (deleted the 70 original data files, 71 manifests and 57 manifest lists). The whole job took 48.8 s including measurements.
- **Independent check:** after the job, a PyIceberg reader (not Spark) confirmed 2,501,500 rows, all event ids distinct, 1 file, 1 snapshot.
- **How much to trust the scan time:** the same unchanged table measured 1.73 s and then 1.41 s within one job, so scan timings have roughly ±20% noise on this setup. A 3.5 times difference is well outside that, but it is one run, on a small table (73 MiB), against local S3 with almost no request latency. Real S3 charges a request per file, so the real-world effect of many small files is larger than shown here.
- Expiry with "older than 0 hours" removes time travel. It was used for the demonstration only; the scheduled job keeps 72 hours and at least 10 snapshots.

## **2. Compaction while Flink is writing**

Script: `local/scripts/spark-compaction-under-load.sh`. It sends 5,000 events/s for 100 s through the pipeline, runs two Spark compaction jobs while that is happening, waits for everything to drain, then compares the table with Kafka.

|                                             | **Result**                                         |
| ------------------------------------------- | -------------------------------------------------- |
| **Load**                                    | 9,500 requests, all HTTP 200, about 5,000 events/s |
| **Compaction 1 (table had 3,009,621 rows)** | 7 files merged into 1, 0 failures                  |
| **Compaction 2 (table had 3,166,609 rows)** | 3 files merged into 1, 0 failures                  |
| **Flink job**                               | RUNNING throughout, 0 pod restarts                 |
| **Kafka messages vs table rows**            | **3,501,500 = 3,501,500**, all event ids distinct  |

Result: **passed.** Compaction needed no pause of the stream; both rewrites committed while the sink kept appending, and nothing was lost or duplicated. Row counts are taken from table metadata at the moment each job started, so they are approximate snapshots of a moving table.

Request latency during this run was higher than a quiet run (p99 40.6 ms against 23.6 ms for the same rate earlier). The likely cause is that Spark pods and compaction competed with the pipeline for the same laptop, but this was not isolated, so it is not a measured cost of compaction.

## **3. Hourly aggregate**

`aggregate.py` over the whole table produced 220 (hour, source, type) groups whose event counts sum to **3,501,500**, equal to the table's row count. The same total came from the scheduled run (below). The unit tests cover correctness, re-runs, late data, window alignment and null values.

### **Scheduled runs**

A `ScheduledSparkApplication` was verified by temporarily setting `events-hourly-aggregate` to run every minute: it fired, the Spark application completed and printed its summary. The schedules were then restored to their real, suspended definitions. The compaction and expiry schedules were applied but not triggered; their jobs were run by hand.

## **4. Shuffle partitions and adaptive execution**

The hourly aggregate over 24 hours (3.5 million input rows) was run with four settings, twice each. "Seconds" is the job's own measurement inside the Spark application (table creation, merge and counts), excluding pod start-up.

| **Setting**                         | **Run 1** | **Run 2** | **Output files** |
| ----------------------------------- | --------- | --------- | ---------------- |
| **AQE off, 200 shuffle partitions** | 40.4 s    | 43.7 s    | 1                |
| **AQE off, 8 shuffle partitions**   | 29.4 s    | 28.6 s    | 1                |
| **AQE on, 200 (the default)**       | 27.8 s    | 26.4 s    | 1                |
| **AQE on, 8 shuffle partitions**    | 28.5 s    | 25.9 s    | 1                |

- Without adaptive query execution, the default of 200 shuffle partitions made the job about 50% slower than 8: the aggregate output is only a few hundred rows, so 200 tasks are mostly scheduling overhead.
- With AQE on (Spark's default), the 200-partition setting is as fast as the hand-tuned one, because AQE merges small shuffle partitions at run time. The practical advice is to leave AQE on rather than tune the partition count per job.
- The output was a single file in every case, so no setting changed file counts here.
- Two runs per setting, one small result set, two single-core executors: this shows the mechanism and its direction, not numbers to carry to other workloads.

## **Fixed after review (2026-11-01)**

- **`dedupe` could leave a duplicate in another day's partition.** It used a dynamic partition overwrite, which replaces only partitions that still have output rows: a copy whose `event_time` was the only row of another day survived. It now overwrites the whole table and fails if any `event_id` is still duplicated afterwards; a test replays a copy onto the next day.
- **The hourly aggregate never removed a group that dropped to zero events.** The `MERGE` only upserted. It now also deletes groups in the window that are gone from the source (`WHEN NOT MATCHED BY SOURCE`, limited to the window); a test covers it.
- Both new tests were checked against the old code: with the dynamic overwrite and without the delete clause, each fails; with the fixes, both pass (full suite: 32 tests).

## **What went wrong (and what it taught)**

1. **Compaction failed with "table not found" on the real table.** The catalog and the namespace are both called `lakehouse`, so the procedure argument `lakehouse.events` was read as catalog `lakehouse`, table `events`. The tests had used a different namespace name and missed it. Fixed by passing the full three-part name, with a regression test using the same-name layout.
2. **MERGE from a DataFrame view failed to plan** ("No plan for TableReference") with Spark 4.1.4 and Iceberg 1.12.0. The aggregation now sits inside the MERGE as a SQL subquery.
3. **Iceberg refuses orphan removal under 24 hours** by design, since young unreferenced files may belong to in-flight writes. My first test used a fake clock and tripped this guard; the test now back-dates a real file.
4. **The scan benchmark's first run was unfairly slow** (a cold start), which would have flattered "after". An untimed warm-up run was added.
5. **The local S3 stand-in failed under combined load, twice.**
   - SeaweedFS was OOM-killed repeatedly at its 1 GiB limit; the limit is now 3 GiB.
   - After the 3-second checkpoint interval had run for a while, SeaweedFS reached its 40-volume cap: every full-state Flink checkpoint wrote hundreds of MB, deleted checkpoints are only reclaimed when SeaweedFS vacuums, and 26 of the 40 volumes were checkpoint data. S3 writes failed, Flink could not checkpoint and stalled, and 500,000 new events waited in Kafka. The volume cap is now 200 and the interval is back to 10 s.
   - My first compaction-under-load test **failed because of this**: the table had 2,501,500 rows while Kafka had 3,001,500, and the two compaction runs found nothing to compact because no new files were arriving. The final comparison caught the gap, which is what it is for.
   - Once S3 was healthy again, Flink drained the whole backlog on its own and the table matched Kafka exactly (3,001,500 distinct rows), so exactly-once survived an extended storage outage. The second run is the one reported above.
6. **The ingestion service stayed "Running" but never ready** after the cluster was restarted, because Kafka's DNS name did not resolve at start-up and the failed start left the JVM alive. It now exits on a failed start so Kubernetes restarts it.

## **Limits of this evidence**

- A laptop with local object storage: nothing here says how the jobs perform on S3 at scale.
- One run per experiment, apart from the tuning table (two runs).
- Compaction under load was tested with two overlapping jobs and one steady load, not with deletes, upserts or several compactions racing each other.
- The expiry and orphan schedules were not run on a long-lived table, so their long-term behaviour (and the 72-hour windows) is untested here.
- Full-state checkpoints of a large de-duplication state remain a cost to resolve before AWS; RocksDB with incremental checkpoints is the likely change.
