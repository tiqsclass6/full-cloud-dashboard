# **Flink to Iceberg job**

Streams events from Kafka (`events.raw`, Avro in the Confluent wire format) into the Iceberg table `lakehouse.events`, exactly once.

```
KafkaSource -> keyBy(event_id) -> Deduplicator (60 min state) -> IcebergSink (Parquet, commit per checkpoint)
```

Runs on `flink:2.1.3-java17` (Java 17 is Flink's recommended runtime; Java 21 is experimental in Flink 2.x), so the jar is built with a Java 17 toolchain.

Design reasoning: [**ADR 0004**](../../docs/architecture-decisions/0004-flink-to-iceberg-pipeline.md). Test results and how to reproduce them: [**docs/flink-iceberg.md**](../../docs/flink-iceberg.md).

## **How the guarantees fit together**
| **Concern**                                  | **Mechanism**                                                                                                                                                                                |
| -------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **No loss after a crash**                    | Kafka offsets and operator state are in Flink checkpoints (10 s, to S3); a restart rewinds to the last completed checkpoint                                                                  |
| **No duplicate rows after a restart**        | The Iceberg sink writes data files, then commits a snapshot only when the checkpoint covering them completes. Files from a failed attempt are never committed, and a re-commit is idempotent |
| **No duplicates from producer retries**      | `Deduplicator` drops repeated `event_id`s using keyed state that is part of every checkpoint and savepoint; the count is `flink_taskmanager_job_task_operator_duplicatesDropped`             |
| **Safe upgrades**                            | `upgradeMode: savepoint`: the operator stops the job with a savepoint and restores from it                                                                                                   |
| **Producers and table evolve independently** | The deserializer reads with the writer schema and maps by column name; see "Schema evolution" below                                                                                          |

## **Configuration (environment variables on the pod)**
| **Variable**                                                  | **Default**                                                                           |
| ------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| **`KAFKA_BOOTSTRAP_SERVERS`**                                 | `lakehouse-kafka-bootstrap.kafka.svc:9092`                                            |
| **`TOPIC` / `CONSUMER_GROUP`**                                | `events.raw` / `flink-events-to-iceberg`                                              |
| **`SCHEMA_REGISTRY_URL`**                                     | `http://apicurio-registry.data.svc:8080/apis/ccompat/v7`                              |
| **`ICEBERG_REST_URI` / `ICEBERG_WAREHOUSE`**                  | `http://iceberg-rest.data.svc:8181` / `s3://warehouse/`                               |
| **`ICEBERG_NAMESPACE` / `ICEBERG_TABLE`**                     | `lakehouse` / `events`                                                                |
| **`S3_ENDPOINT`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`** | unset (set only for the local SeaweedFS; on AWS the default credential chain is used) |
| **`DEDUPE_TTL_MINUTES` / `PARALLELISM`**                      | `60` / `2`                                                                            |

Checkpoint interval, state backend, HA and checkpoint/savepoint locations are values of the `flink-jobs` Helm chart (`helm/charts/flink-jobs/values.yaml`, overridden per environment in `helm/values`).

## **The table**
Created on first start: `lakehouse.events`, format v2, Parquet with zstd, partitioned by `day(event_time)`. Columns: `event_id`, `source`, `type`, `event_time`, `ingest_time`, `value`, `attributes` (map), plus any columns added later.

## **Schema evolution**
- **Producer adds an optional field** (Avro schema v2): the job keeps running. Fields the table does not have yet are ignored.
- **Table gains a column** (`SchemaTool add-column`, metadata only): after a savepoint restart the job fills it; records written before then read back as null.
- Recommended order: table first, then the job restart, then the producers.

```bash
# add a column (run inside the JobManager pod, which already has the config in its environment)
java -cp /opt/flink/usrlib/job.jar io.lakehouse.flink.SchemaTool add-column region string
```

## **Build, test, deploy**
```bash
./gradlew build            # unit tests (deserializer mapping, evolution cases, Deduplicator on a mini-cluster) + fat jar
make local-deploy-flink    # from the repository root: build, load image into kind, restart the job via a savepoint;
                           # returns once the operator reports the new restartNonce as stable and the job RUNNING
```

The unit tests cover the decode and evolution rules and the de-duplication operator. The end-to-end exactly-once behaviour is tested against the real cluster (`make local-test-exactly-once`).

## **Known limits**
- Poison records (not Avro, unknown schema id, a required column missing) are counted (`deserializationErrors` metric) and skipped; there is no dead-letter topic yet. A schema registry that cannot answer (unreachable, timeout, HTTP 5xx or 429) is not a poison record: the task fails and Flink restarts from the last checkpoint, so the record is read again instead of being dropped. A long registry outage therefore shows up as job restarts.
- Hash-map state backend: fine for a 60-minute id set locally, but size it (or move to RocksDB) before AWS.
- Many small files accumulate (one set per checkpoint); compaction arrives with the PySpark jobs in Phase 4.
- The job jar is about 185 MB (AWS SDK and Hadoop client bundled).
- Kubernetes HA is on, with a standby JobManager in another zone (ADR 0004, decision 8). There is one TaskManager, so losing its node still pauses processing for about 2 minutes ([**chaos results**](../../docs/chaos/README.md)).
