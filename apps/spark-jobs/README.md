# **Spark jobs (PySpark)**

Batch jobs that keep the lakehouse healthy and derive data from it. They run on Kubernetes as `SparkApplication`s (Kubeflow Spark operator), read and write Iceberg through the REST catalog, and print one `SUMMARY {...}` JSON line per run.

| **Job**              | **Entry point**       | **Purpose**                                                                                                                                                      |
| -------------------- | --------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Maintenance**      | `jobs/maintenance.py` | Compaction (`rewrite_data_files`, bin-pack), manifest rewrite, snapshot expiry, orphan file removal, with before/after statistics and an optional scan benchmark |
| **Hourly aggregate** | `jobs/aggregate.py`   | Incremental per-hour, per-source, per-type statistics into `events_hourly` (count, sum, min, max, average, approximate p95)                                      |

Design reasoning: [**ADR 0005**](../../docs/architecture-decisions/0005-spark-maintenance-and-aggregates.md). Measured results: [**docs/spark-iceberg.md**](../../docs/spark-iceberg.md).

## **Why the maintenance steps exist**
The Flink sink commits a few small files per checkpoint, so a streaming table fills up with small files, snapshots and manifests.

| **Step**        | **Problem it solves**                                                    | **Safety notes**                                                                                                                                                                        |
| --------------- | ------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`compact`**   | Many small files slow scans and bloat metadata                           | Only adds a snapshot. A rewrite of existing files and a concurrent Flink append both succeed (optimistic concurrency)                                                                   |
| **`manifests`** | Many small manifest files slow query planning                            | Metadata only                                                                                                                                                                           |
| **`expire`**    | Old snapshots keep pre-compaction files alive, so storage is never freed | Bounds time travel to the retention window. `--retain-last` keeps a minimum                                                                                                             |
| **`orphans`**   | Files left by failed writes                                              | Only files older than `--orphan-older-than-hours` (default 72). Iceberg itself refuses anything under 24 h, because unreferenced files younger than that can belong to in-flight writes |

Steps always run in the order compact, manifests, expire, orphans, however they are listed.

## **The aggregate**
- The window is widened to **whole hours**: aggregating half an hour would write a wrong row.
- Results are **merged** on (hour, source, type), so re-running a window replaces its rows. Retries, backfills and overlapping schedules are safe, and late events correct earlier hours on the next run (the default look-back is 3 hours).
- The filter is on `event_time`, which the source table is partitioned by (`days(event_time)`), so only the needed days are read.

## **Run**
```bash
make local-deploy-spark        # build the image (Iceberg runtime baked in), load it into kind
make local-spark-maintenance   # compact + manifests + expire, with before/after stats
make local-spark-aggregate     # hourly aggregate over the last 24 hours
make local-spark-schedules     # list the scheduled jobs (defined in the Helm chart)
```

One-off runs with arguments:
```bash
bash local/scripts/spark-run.sh <name> maintenance --steps compact --min-input-files 2
bash local/scripts/spark-run.sh <name> aggregate --start 2026-10-28T00:00 --end 2026-10-29T00:00
```

### **Schedules (`schedules` in `helm/charts/spark-jobs/values.yaml`, deployed by Argo CD)**
| **Name**                      | **Schedule (UTC)** | **What**                                                               |
| ----------------------------- | ------------------ | ---------------------------------------------------------------------- |
| **`events-compaction`**       | hourly at :00      | compact + manifests, target 128 MiB                                    |
| **`events-expire`**           | daily 03:30        | expire (older than 72 h, keep at least 10) + orphans (older than 72 h) |
| **`events-hourly-aggregate`** | hourly at :10      | last 3 hours                                                           |

Locally they are **suspended** (`spark-jobs.suspend: true` in `helm/values/local.yaml`) so they do not use laptop memory on their own; on AWS they run. To enable them locally, change that value (or add `suspend: false` to one schedule) and `make gitops-push`. A `kubectl patch` would be reverted by Argo CD.

### **Repair: duplicate rows**
`--steps dedupe` keeps one row per `event_id` (the earliest ingested) and rewrites the affected partitions. It is never part of the default steps or a schedule. It exists for incidents such as a stream restored from an old savepoint, and needs the writer stopped first. `local/scripts/repair-duplicates.sh` does the whole runbook: suspend the Flink job, dedupe and compact, resume, verify.

## **Test**
```bash
make local-spark-test      # or: apps/spark-jobs/run-tests.sh [pytest args]
```
The tests run **inside the Spark image** (Linux, Java 21, the same Iceberg jars as production), against a local Iceberg catalog. They cover compaction (rows preserved, idempotent, leaves small partitions alone), expiry with a controllable clock, the orphan safety window, step ordering, the same-name catalog/namespace layout, and the aggregate (correct statistics, idempotent re-runs, late data, hour alignment, window bounds, null values, empty windows). They do not run on the Windows host because Hadoop's local filesystem needs `winutils`.

## **Configuration**
Settings come from environment variables on the driver (`ICEBERG_REST_URI`, `ICEBERG_WAREHOUSE`, `S3_ENDPOINT`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `AWS_REGION`), the same names the Flink job uses. On AWS the S3 variables are unset and the default credential chain applies; the catalog would be Glue instead of REST, which changes only the catalog properties in `lakehouse_jobs/common.py`.

## **Known limits**
- **Aggregates are batch and approximate in one respect:** `value_p95` uses `percentile_approx`.
- **MERGE source:** the aggregate is written as a SQL subquery inside `MERGE`, because feeding `MERGE` from a DataFrame temp view failed to plan with Spark 4.1.4 and Iceberg 1.12.0 ("No plan for TableReference").
- **Namespace named like the catalog:** Iceberg procedures get the full `catalog.namespace.table` name, since a two-part name is ambiguous when the namespace and catalog share a name (here both are `lakehouse`).
- **Scan benchmark** is a rough single-machine indicator with roughly ±20% noise; see the results page.
- **No Prometheus metrics yet** from these jobs; the `SUMMARY` lines are the interface until Phase 8.
