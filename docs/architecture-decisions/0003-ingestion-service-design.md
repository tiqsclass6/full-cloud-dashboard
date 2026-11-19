# **ADR 0003: Ingestion service design**

- Status: accepted
- Date: 2026-10-29

## **Context**
Phase 2 needs a Java service that accepts events over HTTP and writes them to Kafka. The role this platform targets stresses distributed-systems design, concurrency, networking and JVM tuning, so the service should expose those decisions rather than hide them behind a framework.

## **Decisions**
1. **Plain Java 21 and the JDK HTTP server, no Spring.** The request path is small, and the dependency list is Kafka client, Avro, Jackson and Micrometer. This keeps the networking, threading and memory behaviour visible and profileable. A framework can be adopted later if the surface grows.
2. **Virtual threads for request handling.** Handlers block waiting for broker acknowledgements. Virtual threads make that cheap without a hand-sized pool.
3. **Respond after acknowledgement (`acks=all`).** A 200 means the event is durable on the in-sync replicas. The cost is latency (dominated by the linger plus broker round trip), accepted for correctness.
4. **Bounded in-flight work with a semaphore, answering 429 when full.** It gives memory a hard ceiling and clients an explicit retry signal, instead of an unbounded queue that fails late.
5. **Confluent wire format over Apicurio's compatibility API, implemented directly.** Registration is one HTTP call, and the encoder is about 20 lines. Flink, Spark and Kafka Connect then read the topic with standard Confluent-format deserializers, and the service does not depend on registry-vendor client libraries.
6. **At-least-once with client-supplied `event_id`.** A partially failed batch may leave some events in Kafka; retries reuse the ids and downstream jobs de-duplicate (Flink and Iceberg in Phase 3).
   - *Amended 2026-11-18:* `event_id` is now **required**. The service used to generate one when it was missing and return it in the reply, but a source whose reply was lost never learned that id, so its retry became a second row. A retry is only recognisable if the source chose the id before the first send. Retries must also arrive within the Flink job's de-duplication window (60 minutes, [**ADR 0004**](0004-flink-to-iceberg-pipeline.md)). The `ingestion-handoff` chaos experiment tests both lost-reply cases.
7. **Separate management port** for metrics and probes, so scraping never competes with ingest traffic.
8. **JVM settings are explicit.** G1 is selected explicitly, because a container under 2 CPUs or 1792 MiB would otherwise get SerialGC. Heap is 70% of the memory limit. JFR runs continuously in the default deployment.

## **Consequences**
- More code to own than a framework would need (validation, metrics wiring, shutdown), in exchange for control and profiling clarity.
- The service has no authentication yet; it relies on network placement until Phase 7.
- GC choice (G1 versus ZGC) and the producer's compression behaviour are measured in `docs/performance`, not assumed.
- A failed start-up (for example Kafka's DNS name not resolving right after a cluster restart) makes the process exit, so Kubernetes restarts it. Earlier, the management server kept the JVM alive and left a pod that was Running but never Ready.
