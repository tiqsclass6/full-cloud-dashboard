# **Ingestion service: JVM performance baseline**

First performance baseline for the [**ingestion service**](../../apps/ingestion-service/README.md), measured with Java Flight Recorder (JFR) on the local kind cluster. Its purpose is to establish a method and a starting point, not to publish production numbers.

## **How it was measured**

- **Setup:** 1 replica (limits: 2 CPUs, 1 GiB; heap = 70% of memory, about 700 MiB), JDK 21 (Temurin), on the 4-node kind cluster described in the root README. The service writes to the 3-broker Strimzi Kafka with `acks=all`, idempotence on, LZ4 compression, 5 ms linger and 64 KiB batches.
- **Load:** the in-cluster open-loop generator at **30,000 events/s** (batches of 50, so 600 requests/s), 64 workers, 70 s with the first 15 s discarded as warm-up. Latency is measured from the *intended* send time, so stalls show up as latency instead of silently reducing the rate.
- **Profiling:** JFR `profile` settings for the whole run. `local/tools/jfr_summary.py` summarises only the measured 55 s window.
- **Reproduce:** `make local-perf LABEL=my-run GC=g1` (or `GC=zgc`). Output goes to `docs/performance/results/<label>.json`; raw `.jfr` files stay in `docs/performance/data/` (git-ignored). Open them in JDK Mission Control.

## **Results**

|                                       | **G1 (baseline)**    | **Generational ZGC**  | **G1 + Avro direct encoder**                                              |
| ------------------------------------- | -------------------- | --------------------- | ------------------------------------------------------------------------- |
| **Throughput achieved**               | 30,000 events/s      | 30,000 events/s       | 30,000 events/s                                                           |
| **HTTP errors**                       | 0                    | 0                     | 0                                                                         |
| **Request latency p50 / p99 / p99.9** | 6.6 / 23.1 / 36.5 ms | 6.7 / 23.0 / 32.7 ms  | 6.3 / 21.8 / 29.5 ms                                                      |
| **GC collections per second**         | 3.7                  | 0.6                   | 4.4 (more, smaller young collections; total pause time is about the same) |
| **GC pause: median / p99 / max**      | 1.3 / 2.3 / 2.7 ms   | 0.04 / 0.15 / 0.15 ms | 1.2 / 2.0 / 2.2 ms                                                        |
| **Time paused in GC**                 | 0.51%                | 0.007%                | 0.55%                                                                     |
| **Peak heap used**                    | 103 MiB              | 670 MiB               | 79 MiB                                                                    |
| **Median live set after GC**          | 20 MiB               | 88 MiB                | 22 MiB                                                                    |
| **JVM CPU (share of the machine)**    | 3.6%                 | 3.5%                  | 3.2%                                                                      |
| **Allocation rate (sampled)**         | 303 MiB/s            | 322 MiB/s             | **245 MiB/s**                                                             |

Full per-run numbers, including top allocation sites and hot methods, are in [**`results/`**](results/).

## **What the profile showed**

1. **Allocation, not pauses, was the cost to look at.** About 50% of all allocation was `Lz4BlockOutputStream.<init>`: the Kafka client allocates fresh compression buffers for every batch. A further 17% was `BufferedBinaryEncoder.configure`: the Avro encoder allocated a 2 KiB scratch buffer on every event.
2. **Fix applied:** `ConfluentAvroEncoder` now uses Avro's `directBinaryEncoder`, which writes straight into the in-memory stream. Result: that allocation site disappeared and the sampled allocation rate fell about 19% (303 to 245 MiB/s). JVM CPU fell slightly (3.6% to 3.2%).
3. **G1 versus ZGC:** generational ZGC removes GC pauses almost entirely (max 0.15 ms against 2.7 ms), but request latency is the same in both. At this load GC is not the bottleneck: end-to-end latency is dominated by the producer linger (5 ms) plus the `acks=all` round trip to three brokers.
4. **ZGC memory cost:** ZGC used up to 670 MiB of the roughly 700 MiB heap (it does not collect aggressively when there is room) versus about 100 MiB for G1. With a 1 GiB container limit this leaves little margin, so G1 stays the default and ZGC would need a larger limit.
5. **The remaining top allocator is Kafka's own LZ4 buffers.** Options for a later pass are a longer linger (fewer, larger batches), a different codec, or no compression on a fast network. I have not tried them.

## **Caveats**

- **Single run per configuration.** Differences of about 1 ms in p99 or a few percent in allocation are within run-to-run noise. Only the allocation reduction is clearly attributable to the change, because the allocation site itself vanished from the profile.
- **Shared laptop.** The load generator, 3 Kafka brokers, Prometheus and the service share the same host CPUs. The JVM used only about 3.5% of the machine, so the service had plenty of headroom: in earlier ad hoc runs two replicas also sustained 60,000 events/s with zero errors.
- **No real network.** Pod-to-pod traffic on one host has almost no latency, so p50 here understates a multi-AZ deployment.
- **Not a capacity statement.** The numbers set a method and baseline to compare against on EKS in Phase 6.
- **Samples, not totals.** JFR allocation and execution events are sampled; treat percentages as indicative.

## **Re-run under GitOps (Phase 5)**

`perf-run.sh` now pauses Argo CD's automated sync while it reconfigures the deployment, and resumes it afterwards, which restores the deployment from Git. On a freshly built cluster (`g1-gitops`, same settings as the G1 runs above):

|                                       | **G1 + Avro direct encoder (Phase 2)** | **`g1-gitops` (Phase 5)** |
| ------------------------------------- | -------------------------------------- | ------------------------- |
| **Throughput, errors**                | 30,000 events/s, 0                     | 30,000 events/s, 0        |
| **Request latency p50 / p99 / p99.9** | 6.3 / 21.8 / 29.5 ms                   | 7.2 / 24.9 / 38.6 ms      |
| **GC pause median / p99 / max**       | 1.2 / 2.0 / 2.2 ms                     | 1.5 / 3.5 / 6.2 ms        |
| **Allocation rate (sampled)**         | 245 MiB/s                              | 239 MiB/s                 |
| **JVM CPU (share of the machine)**    | 3.2%                                   | 4.1%                      |

The allocation profile is unchanged. Latency and GC pauses are somewhat higher in this single run, on a cluster that now also runs Argo CD, Gitea and the Spark operator; the run is not repeated, so this is not a measured regression.
