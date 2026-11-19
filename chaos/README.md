# **Chaos experiments**

Fault-injection experiments for the platform, run by `local/scripts/chaos-run.sh <experiment>`. Chaos Mesh (`gitops/operators/chaos-mesh`) injects the faults; it can only target namespaces annotated `chaos-mesh.org/inject=enabled` (`apps`, `data`, `kafka`).

The manifests are versioned here but **not synced by Argo CD**: an experiment runs only when the harness applies it, never because of a sync. The same experiments are the scenarios the Phase 9 ops agent is evaluated against.

| **Experiment**               | **Fault**                                                                                                | **Must hold**                                                              |
| ---------------------------- | -------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| **`broker-isolation`**       | 100% packet loss on one Kafka broker for 6 minutes                                                       | `KafkaBrokerDown` fires; ingestion keeps its SLO; every event exactly once |
| **`flink-taskmanager-kill`** | A Flink TaskManager killed under load                                                                    | The job recovers from its checkpoint; the table stays fresh; exactly once  |
| **`kafka-latency`**          | 400 ms delay from the Flink job to the brokers for 6 minutes                                             | Lag rises and then drains; every event exactly once                        |
| **`zone-outage`**            | A whole kind node (one zone) paused for 6 minutes (`docker pause`; harness only)                         | `KafkaBrokerDown` fires; the pipeline continues on two zones; exactly once |
| **`ingestion-handoff`**      | An ingestion pod killed every 30 s for 6 minutes; the client also discards the reply of every 20th batch | Retried copies reach Kafka and Flink drops them; every event exactly once  |

Each run sends steady traffic with its own range of event ids, records which alerts fire and when, measures recovery, verifies the ids in Iceberg, and writes `docs/chaos/results/<experiment>.json`. Results and runbook links: [**docs/chaos/README.md**](../docs/chaos/README.md).
