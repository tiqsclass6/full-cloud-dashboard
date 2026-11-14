# **Iceberg table-health exporter**

Inspects the lakehouse's Iceberg tables on a fixed interval and serves their health as Prometheus metrics. It reads table metadata and manifests only, never data files, through the same catalog as the jobs (REST locally, the Glue Data Catalog on AWS).

## **Metrics**

All labelled `table="namespace.name"`. Timestamps are epoch seconds and `NaN` until the event has happened; alerts compute ages with `time() - ...`.

| **Metric**                                              | **Meaning**                                                                  |
| ------------------------------------------------------- | ---------------------------------------------------------------------------- |
| **`iceberg_table_up`**                                  | 1 if the last inspection succeeded                                           |
| **`iceberg_table_snapshots`**                           | Snapshots in the metadata (grows until the Spark job expires them)           |
| **`iceberg_table_data_files`**                          | Data files in the current snapshot                                           |
| **`iceberg_table_small_data_files`**                    | Data files below `SMALL_FILE_BYTES` (compaction candidates)                  |
| **`iceberg_table_delete_files`**                        | Delete files in the current snapshot                                         |
| **`iceberg_table_records`, `iceberg_table_size_bytes`** | Rows and bytes in the current snapshot                                       |
| **`iceberg_table_manifests`**                           | Manifest files in the current snapshot                                       |
| **`iceberg_table_last_commit_timestamp_seconds`**       | Time of the current snapshot: data freshness                                 |
| **`iceberg_table_last_compaction_timestamp_seconds`**   | Time of the newest compaction (a `replace` snapshot that removed data files) |
| **`iceberg_exporter_inspections_total{result}`**        | Inspections, by success or failure                                           |
| **`iceberg_exporter_inspection_duration_seconds`**      | Time to inspect a table                                                      |

A failed inspection sets `iceberg_table_up` to 0 and keeps the last known values; one failing table never stops the others.

## **Configuration**

| **Variable**                                                                              | **Default**                                                      |
| ----------------------------------------------------------------------------------------- | ---------------------------------------------------------------- |
| **`TABLES`**                                                                              | `lakehouse.events` (comma-separated)                             |
| **`INTERVAL_SECONDS`**                                                                    | `60`                                                             |
| **`SMALL_FILE_BYTES`**                                                                    | 32 MiB (a quarter of the compaction target, as in the Spark job) |
| **`MANAGEMENT_PORT`**                                                                     | `9400` (`/metrics`, `/health/live`, `/health/ready`)             |
| **`ICEBERG_CATALOG_TYPE`, `ICEBERG_REST_URI`, `ICEBERG_WAREHOUSE`, `AWS_REGION`, `S3_*`** | Same as the Flink and Spark jobs                                 |

## **Build and test**

```bash
./gradlew build installDist
```

Unit tests commit real appends and a compaction to an in-memory Iceberg catalog and check what the inspector reports.
