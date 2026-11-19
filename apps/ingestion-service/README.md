# **Ingestion service**

Java 21 HTTP service that validates telemetry events, encodes them as Avro in the Confluent wire format and publishes them to Kafka (`events.raw`) with `acks=all`. A request returns only after the brokers have acknowledged every event.

```
POST /v1/events  --> validate --> back-pressure --> Avro encode --> Kafka (key = source) --> 200 after ack
```

## **API**
`POST /v1/events` with one JSON object or an array (up to 500):

```json
{"event_id": "3f2b8c1e-5c1a-4c55-9a53-1b2f6b1f0c11", "source": "host-1", "type": "cpu.usage", "value": 0.42,
 "timestamp": 1793232000000, "region": "us-east-1", "attributes": {"host": "a"}}
```

`event_id` (a UUID), `source` and `type` are required; the rest is optional. The source chooses the id **before the first send** and keeps it for every retry of that event: that is what lets the pipeline recognise a retry as a copy.

| **Status** | **Meaning**                                                                                                            |
| ---------- | ---------------------------------------------------------------------------------------------------------------------- |
| **200**    | Every event acknowledged by Kafka. Body: `{"accepted": n, "event_ids": [...]}`                                         |
| **400**    | Validation failed (including a missing `event_id`) or malformed JSON (including a `null` event). Nothing was published |
| **413**    | Body larger than `MAX_BODY_BYTES`                                                                                      |
| **429**    | In-flight limit reached (`Retry-After: 1`). Retry with the same `event_id`                                             |
| **503**    | Kafka did not acknowledge in time or failed (`Retry-After: 2`). Some events may be stored: retry with the same ids     |

A 503 body is `{"failed": n, "reason": ..., "event_ids": [...], "failed_event_ids": [...]}`, with every id in request order.

### **Retry contract**

| **What the source sees**                                | **What it does**                                     |
| ------------------------------------------------------- | ---------------------------------------------------- |
| **200**                                                 | Done: every event is on all in-sync Kafka replicas   |
| **429, 503, a timeout, a dropped connection, no reply** | Retry the same events with the same `event_id`s      |
| **400, 413**                                            | Fix the request; retrying it unchanged will not help |

- **Why a retry is safe.** Without a 200 the source cannot know whether Kafka already has the events (the reply can be lost after the brokers acknowledged them). A retry may therefore write a second copy to Kafka; the Flink job drops every copy of an `event_id` it has already seen, so the table gets each event once.
- **The retry window is 60 minutes** (`DEDUPE_TTL_MINUTES` in the Flink job). Flink forgets an id 60 minutes after it first saw it, so a retry that arrives later than that is stored a second time. Sources should give up, and report the events as undelivered, well inside that window.
- **The source must retry.** An event the source drops after a failure is lost; no 200 means "not stored yet".
- **Tested by** the `ingestion-handoff` chaos experiment ([**results**](../../docs/chaos/README.md)): ingestion pods killed every 30 seconds and the reply of every 20th batch thrown away unread, with every event checked in Iceberg.

The management port (8081) serves `/metrics` (Prometheus), `/health/live` and `/health/ready`.

## **Design notes**
- **Delivery:** `acks=all`, idempotent producer, key = `source` (per-source ordering within a partition). The idempotent producer stops the Kafka client's own retries from writing duplicates; it cannot see a retry by the HTTP client, which is a new request. End to end the API is *at least once*; the Flink job de-duplicates on `event_id`.
- **Client-chosen ids only.** The service used to generate an id when the source sent none, and returned it in the reply. If that reply was lost, the source's retry got a new id and became a second row. Events without `event_id` are now rejected (400).
- **Schema:** `src/main/resources/schemas/event.avsc` (v2; adds the optional `region`; `event-v1.avsc` is the original), registered at startup under subject `<topic>-value` in Apicurio through its Confluent-compatible API. Records use the Confluent wire format (magic byte, 4-byte schema id, Avro binary) so Flink and Spark can read them with standard deserializers.
- **Concurrency:** each request runs on a virtual thread, so blocking while waiting for a Kafka acknowledgement is cheap and there is no worker pool to size.
- **Back-pressure:** a semaphore bounds events that are sent but not yet acknowledged (`MAX_IN_FLIGHT`, default 10,000). When it is exhausted the service answers 429 immediately instead of queueing without bound, keeping memory flat.
- **Observability:** Micrometer exposes JVM (memory, GC, threads), Kafka client, and application metrics: `ingest_requests_total{outcome}`, `ingest_events_total{outcome}`, `ingest_inflight`, `ingest_produce_latency_seconds` and `ingest_request_latency_seconds` (histograms with SLO buckets).

## **Configuration (environment variables)**
| **Variable**                                              | **Default**                                                                                                                 |
| --------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| **`KAFKA_BOOTSTRAP_SERVERS`**                             | `localhost:9092`                                                                                                            |
| **`SCHEMA_REGISTRY_URL`**                                 | `http://localhost:8085/apis/ccompat/v7`                                                                                     |
| **`TOPIC`**                                               | `events.raw`                                                                                                                |
| **`HTTP_PORT` / `MANAGEMENT_PORT`**                       | `8080` / `8081`                                                                                                             |
| **`MAX_IN_FLIGHT` / `MAX_BATCH_SIZE` / `MAX_BODY_BYTES`** | `10000` / `500` / `1048576`                                                                                                 |
| **`PUBLISH_TIMEOUT_MS`**                                  | `10000`                                                                                                                     |
| **`SCHEMA_RESOURCE`**                                     | `/schemas/event.avsc`. Set to `/schemas/event-v1.avsc` to behave like an older producer (used by the schema-evolution demo) |
| **`PRODUCER_*`**                                          | Passed to the Kafka producer, e.g. `PRODUCER_LINGER_MS=10` sets `linger.ms`                                                 |

## **Build and test**
Requires a JDK 17+ to run Gradle; the build provisions JDK 21 automatically.

```bash
./gradlew build            # compile (warnings are errors) + unit tests
./gradlew integrationTest  # Testcontainers: real Kafka broker, needs Docker
./gradlew installDist      # runnable distributions in service/ and load-generator/ build/install
```

Unit tests cover validation (including the required `event_id`), the wire format round trip, the registry client, and the HTTP layer including back-pressure (429), Kafka failure (503) and acknowledgement timeout. The integration test publishes 50 events to a real broker, consumes and decodes them, checks per-source partition affinity, and checks the metrics.

## **Run on the local kind cluster**
From the repository root (cluster from `make local-up`):

```bash
make local-deploy-ingest          # build, load the image into kind, deploy 2 replicas
make local-loadtest RATE=10000    # in-cluster load generator; prints latency percentiles
make local-perf LABEL=run1 GC=g1  # JFR-profiled single-replica run (see docs/performance)
```

`load-generator` (command `loadgen`) is an open-loop generator (`--rate --duration --warmup --batch --workers --sources`) that measures latency from each request's intended send time to avoid coordinated omission.

## **Known limits**
- Invalid events are rejected with 400 and not stored; there is no dead-letter path yet (the `events.dlq` topic exists for it).
- No authentication on the ingest port. Default-deny network policies limit which namespaces can reach it; an authenticated API layer (planned with Cognito) is not built.
- The container image uses a JDK base for `jcmd`/JFR. A JRE image with an ephemeral debug container is the production choice.
