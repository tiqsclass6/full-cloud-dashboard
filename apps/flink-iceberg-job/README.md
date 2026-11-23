# **Flink to Iceberg job**

Streams events from Kafka (`events.raw`, Avro in the Confluent wire format) into the Iceberg table `lakehouse.events`, exactly once. Every record it reads ends up in exactly one table: `events`, or `events_skipped` (a duplicate copy, or a record it could not decode), each row carrying its Kafka partition and offset.

```
KafkaSource (+ partition, offset, timestamp) -> route-undecodable -> keyBy(event_id) -> Deduplicator (60 min state) -> IcebergSink events
                                                    \ undecodable                          \ duplicate copies  -> IcebergSink events_skipped
                                                                                                                 (both commit per checkpoint)
```

Runs on `flink:2.1.3-java17` (Java 17 is Flink's recommended runtime; Java 21 is experimental in Flink 2.x), so the jar is built with a Java 17 toolchain.

Design reasoning: [**ADR 0004**](../../docs/architecture-decisions/0004-flink-to-iceberg-pipeline.md). Test results and how to reproduce them: [**docs/flink-iceberg.md**](../../docs/flink-iceberg.md).

## **How the guarantees fit together**
| **Concern**                                  | **Mechanism**                                                                                                                                                                                                                                                                                                       |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **No loss after a crash**                    | Kafka offsets and operator state are in Flink checkpoints (10 s, to S3); a restart rewinds to the last completed checkpoint                                                                                                                                                                                         |
| **No duplicate rows after a restart**        | The Iceberg sink writes data files, then commits a snapshot only when the checkpoint covering them completes. Files from a failed attempt are never committed, and a re-commit is idempotent                                                                                                                        |
| **No duplicates from producer retries**      | `Deduplicator` drops repeated `event_id`s using keyed state that is part of every checkpoint and savepoint; the count is `flink_taskmanager_job_task_operator_duplicatesDropped`                                                                                                                                    |
| **Safe upgrades**                            | `upgradeMode: savepoint`: the operator stops the job with a savepoint and restores from it                                                                                                                                                                                                                          |
| **Every Kafka record accounted for**         | Each row of `events` has `kafka_partition`, `kafka_offset`, `kafka_timestamp`; everything else read goes to `events_skipped` with its reason. The hourly Spark reconciliation checks that each offset is stored exactly once ([**ADR 0011**](../../docs/architecture-decisions/0011-every-offset-accounted-for.md)) |
| **Producers and table evolve independently** | The deserializer reads with the writer schema and maps by column name; see "Schema evolution" below                                                                                                                                                                                                                 |

## **Configuration (environment variables on the pod)**
| **Variable**                                                  | **Default**                                                                           |
| ------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| **`KAFKA_BOOTSTRAP_SERVERS`**                                 | `lakehouse-kafka-bootstrap.kafka.svc:9092`                                            |
| **`TOPIC` / `CONSUMER_GROUP`**                                | `events.raw` / `flink-events-to-iceberg`                                              |
| **`SCHEMA_REGISTRY_URL`**                                     | `http://apicurio-registry.data.svc:8080/apis/ccompat/v7`                              |
| **`ICEBERG_REST_URI` / `ICEBERG_WAREHOUSE`**                  | `http://iceberg-rest.data.svc:8181` / `s3://warehouse/`                               |
| **`ICEBERG_NAMESPACE` / `ICEBERG_TABLE`**                     | `lakehouse` / `events`                                                                |
| **`ICEBERG_SKIPPED_TABLE`**                                   | `events_skipped`                                                                      |
| **`S3_ENDPOINT`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`** | unset (set only for the local SeaweedFS; on AWS the default credential chain is used) |
| **`DEDUPE_TTL_MINUTES` / `PARALLELISM`**                      | `60` / `2`                                                                            |

Checkpoint interval, state backend, HA and checkpoint/savepoint locations are values of the `flink-jobs` Helm chart (`helm/charts/flink-jobs/values.yaml`, overridden per environment in `helm/values`).

## **The tables**
Created on first start, format v2, Parquet with zstd:

- **`lakehouse.events`**, partitioned by `day(event_time)`. Columns: `event_id`, `source`, `type`, `event_time`, `ingest_time`, `value`, `attributes` (map), the lineage columns `kafka_partition`, `kafka_offset`, `kafka_timestamp` (added at start-up by a metadata-only schema change; rows written before read back null), plus any columns added later.
- **`lakehouse.events_skipped`**, partitioned by `day(kafka_timestamp)`: `kafka_partition`, `kafka_offset`, `kafka_timestamp`, `reason` (`duplicate` or `undecodable`), `event_id` and `source` (when decoded), `error`, `payload` (the raw record value, kept only for undecodable records), `recorded_at`.

## **Schema evolution**
- **Producer adds an optional field** (Avro schema v2): the job keeps running. Fields the table does not have yet are ignored.
- **Table gains a column** (`SchemaTool add-column`, metadata only): after a savepoint restart the job fills it; records written before then read back as null.
- Recommended order: table first, then the job restart, then the producers.

```bash
# add a column (run inside the JobManager pod, which already has the config in its environment)
java -cp /opt/flink/usrlib/job.jar io.lakehouse.flink.SchemaTool add-column region string
```

## **Replaying records**
`ReplayTool` sends records the lakehouse does not have back through the ingestion API, with their original `event_id` and the attribute `lakehouse.replayed_from` ([**ADR 0012**](../../docs/architecture-decisions/0012-replay-through-the-api.md)). `local/scripts/replay.sh` runs it as a one-off pod with the JobManager's image, environment and Kafka certificate. It is a dry run unless `--apply` is given.

```bash
bash local/scripts/replay.sh missing --partition 3 --offset 276204 --count 7   # offsets the reconciliation reported
bash local/scripts/replay.sh undecodable --since 2026-11-19T00:00:00Z          # events_skipped rows that decode now
```

Each offset gets one outcome: `REPLAY`; `STORED`, `SKIPPED`, `ALREADY_REPLAYED` or `EVENT_ID_STORED` (nothing to do); `WAIT_FOR_DEDUPE_TTL` (Flink would still drop the copy; retry at the time printed); or `NOT_IN_KAFKA`, `UNDECODABLE` or `NOT_SENDABLE` (only the source can resend it). The decision rules are unit-tested (`ReplayTest`); the Kafka and Iceberg lookups are tested by the integrity drill.

## **Build, test, deploy**
```bash
./gradlew build            # unit tests (decoding, lineage, evolution, the routing of every offset on a mini-cluster) + fat jar
make local-deploy-flink    # from the repository root: build, load image into kind, restart the job via a savepoint;
                           # returns once the operator reports the new restartNonce as stable and the job RUNNING
```

The unit tests cover the decode and evolution rules, the lineage columns, and that a stream with duplicates and an undecodable record leaves every offset in exactly one table. The end-to-end behaviour is tested against the real cluster (`make local-test-exactly-once`, `make local-test-integrity`).

## **Known limits**
- Records that cannot be decoded (not Avro, unknown schema id, a required column missing) do not stop the job: they are counted (`deserializationErrors`), logged, and stored in `events_skipped` with the error and their bytes, and `FlinkRecordsUndecodable` fires. Nothing replays them automatically; once they decode, `local/scripts/replay.sh undecodable` sends them again through the API ([**runbook**](../../docs/runbooks/pipeline.md#flinkrecordsundecodable), [**ADR 0012**](../../docs/architecture-decisions/0012-replay-through-the-api.md)). A schema registry that cannot answer (unreachable, timeout, HTTP 5xx or 429) is not a bad record: the task fails and Flink restarts from the last checkpoint, so the record is read again instead of being set aside. A long registry outage therefore shows up as job restarts.
- Hash-map state backend: fine for a 60-minute id set locally, but size it (or move to RocksDB) before AWS.
- Many small files accumulate (one set per checkpoint); compaction arrives with the PySpark jobs in Phase 4.
- The job jar is about 185 MB (AWS SDK and Hadoop client bundled).
- Kubernetes HA is on, with a standby JobManager and a spare TaskManager, each preferably in another zone, and a 20 s heartbeat timeout (ADR 0004, decisions 8 to 10). In the zone-outage experiment no checkpoint completed for 55-100 s, depending on what the failed node ran ([**chaos results**](../../docs/chaos/README.md)).
