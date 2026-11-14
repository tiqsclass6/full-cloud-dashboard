# **Runbook: Iceberg tables and Spark jobs**

Alerts from the `iceberg` and `platform` groups in `helm/charts/observability/rules/lakehouse.yaml`. Dashboard: **Lakehouse / Tables**. Table health comes from the iceberg-exporter (`apps/iceberg-exporter`).

## **IcebergExporterDown**

**Meaning:** the exporter has not been able to load a table for 15 minutes.
**Impact:** the other Iceberg alerts are blind for that table.
**Diagnose:** `kc -n apps logs deploy/iceberg-exporter` (the failure is logged per table). A table that does not exist yet (`events_hourly` before the first aggregate run) also shows as down.
**Fix:** catalog or storage access (network policy, IRSA on AWS), or create the table by running the job that owns it.

## **IcebergSmallFilesHigh**

**Meaning:** over 1,000 data files below 32 MiB for 30 minutes.
**Impact:** every scan opens many small files: slower queries, higher S3 request cost, larger manifests.
**Fix:** check that the hourly compaction (`events-compaction`) is scheduled and succeeding (SparkJobFailed); run it once by hand: `bash local/scripts/spark-run.sh compact maintenance --steps compact,manifests`.

## **IcebergCompactionOverdue**

**Meaning:** no compaction for over 6 hours while small files keep accumulating.
**Impact:** query performance may degrade and S3 request costs increase.
**Fix:** as IcebergSmallFilesHigh. A table that was never compacted does not fire this alert (the timestamp is NaN).

## **IcebergSnapshotsAccumulating**

**Meaning:** over 2,000 snapshots in the table metadata for an hour.
**Impact:** the metadata file grows with every commit (every 10 s), and data files replaced by compaction are never deleted.
**Fix:** check the daily `events-expire` job; run it by hand: `bash local/scripts/spark-run.sh expire maintenance --steps expire,orphans`.

## **SparkJobFailed**

**Meaning:** a Spark application failed in the last hour.
**Diagnose:** `kc -n apps get sparkapplications`; the driver log in Loki: `{namespace="apps", pod=~"<app>-driver"}`; `kc -n apps describe sparkapplication <app>` for submission errors. Admission rejections (Pod Security, Kyverno) appear in the application's error message.
**Fix:** depends on the cause; the schedule retries on its next run, and failed runs are kept (3) for inspection.
