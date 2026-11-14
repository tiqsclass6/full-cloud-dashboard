# **Chaos experiments: results**

Four fault-injection experiments against the local platform (kind, three worker nodes in three zones), run by `local/scripts/chaos-run.sh` on 2026-10-31. Experiment definitions: [**chaos/README.md**](../../chaos/README.md). Raw results: [**results/**](results/).

Each run sends 100 events per second through the ingestion service for 11 minutes, with a fresh range of event ids. The fault runs from minute 2 to minute 8. Afterwards the harness checks that **every one of those ids is in the Iceberg table exactly once**.

## **Results**

| **Experiment**             | **Fault**                                       | **Events sent** | **In Iceberg (missing / duplicates)** | **Peak consumer lag** | **Recovered after fault ends** | **Alerts fired (seconds after the fault started)**                                                          | **Result** |
| -------------------------- | ----------------------------------------------- | --------------- | ------------------------------------- | --------------------- | ------------------------------ | ----------------------------------------------------------------------------------------------------------- | ---------- |
| **broker-isolation**       | One broker: 100% packet loss, 6 min             | 63,550          | 63,550 (0 / 0)                        | 800                   | 216 s                          | KafkaBrokerDown 222, KafkaNoActiveController 254, KafkaUnderReplicatedPartitions 409                        | Passed     |
| **flink-taskmanager-kill** | A TaskManager killed under load                 | 65,950          | 65,950 (0 / 0)                        | 4,050                 | 197 s                          | FlinkCheckpointFailures 64                                                                                  | Passed     |
| **kafka-latency**          | 400 ms delay, Flink to brokers, 6 min           | 66,000          | 66,000 (0 / 0)                        | 271                   | 225 s                          | none                                                                                                        | Passed     |
| **zone-outage**            | Node `lakehouse-worker3` (zone-c) paused, 6 min | 64,150          | 64,150 (0 / 0)                        | 31,800                | 192 s                          | FlinkJobRestarting 96, KafkaBrokerDown 190, KafkaUnderReplicatedPartitions 393, AlertmanagerClusterDown 461 | Passed     |

How to read the table:

- **Recovery** is measured from the end of the fault to the moment the job is running and consumer lag is zero. The load continues for 3 minutes after the fault by design, so most of each figure is that tail; the backlog itself drained within seconds of the load ending.
- **Ingestion** never failed a request in any experiment. Load shedding and Kafka errors would have shown as retried batches: 2 in the broker isolation, 1 in the zone outage, 0 otherwise.
- **`IcebergCompactionOverdue`** was firing throughout and is left out of the table. It was real (the scheduled compaction is suspended locally) and unrelated to the faults; running the compaction cleared it.

## **What the experiments found**

| **Finding**                                                                                                                                                                                                                                                                                                                    | **Change**                                                                                                 |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------- |
| **Alertmanager had one replica, on the failed node (zone outage): alert notifications would have stopped for the whole outage while Prometheus still detected it**                                                                                                                                                             | AWS runs two Alertmanagers spread across zones (`gitops/operators/kube-prometheus-stack/aws.yaml`)         |
| **A partition that never receives data reports lag -1 (the test traffic's 20 source keys never hash to partition 2), so lag sums never reached 0**                                                                                                                                                                             | Lag queries keep only `>= 0` (alert rule, dashboards, harness); a promtool test covers it                  |
| **Chaos Mesh's own webhook rejected requests: its chart generates a new CA on every render, and under Argo CD the certificate and the webhook's CA came from different renders**                                                                                                                                               | The operators ApplicationSet ignores those generated fields and keeps the first applied values             |
| **`pod-failure` is blocked by the admission policies (it rewrites a pod's image)**                                                                                                                                                                                                                                             | The experiments use faults that leave pod specs alone: packet loss, latency, pod kill, a paused node       |
| **On kind, the KRaft controller quorum re-elects about 190 times an hour, before and during the experiments. Docker Desktop's VM clock is corrected about every 34 seconds (297 seconds of drift an hour), and each step exceeds Kafka's 2-second controller fetch timeout. This is also why `KafkaNoActiveController` fired** | A property of this machine's VM, not the platform: data was never affected. Fix the host clock (see below) |
| **Docker Desktop resumed a paused node by itself once, after 4 minutes 53 seconds**                                                                                                                                                                                                                                            | The harness now checks the node's real state during the fault and records an external resume               |
| **The harness itself had Windows-specific bugs: `jq.exe` ends lines with `\r\n`, it cannot open MSYS paths, and a failing step in the cleanup trap skipped the rest**                                                                                                                                                          | `jq` wrapper in `local/scripts/lib.sh`, work files under `build/`, cleanup that always runs every step     |
| **400 ms of latency barely mattered at 100 events/s: peak lag 271, checkpoints under 1.4 s. The Kafka consumer pipelines fetches, so latency at this rate is absorbed**                                                                                                                                                        | None needed. A heavier load or more latency would be needed to push this past the alert thresholds         |

### **The clock problem on this machine**

Node boot times (`node_boot_time_seconds`) jumped about 106 times in an hour on every kind node. A clean restart of the VM usually stops the drift for a while:

```bash
wsl --shutdown
```

Then start Docker Desktop again; the kind nodes restart with it, and Argo CD and the Flink job recover by themselves (Flink from its latest checkpoint).

## **What these results do not show**

- **Single runs.** Each experiment ran once after the harness was fixed, so the timings are examples, not distributions.
- **A paused kind node is not a real availability zone.** All nodes share one host, one disk and one network, so the zone outage tests Kubernetes and Kafka behaviour, not real AWS failure modes.
- **No AWS run.** Phase 8 was verified locally by choice; the AWS overlays render and validate, but the experiments did not run on EKS.
- **The broker isolation drops the broker's outgoing traffic only.** That makes it unreachable in practice (no TCP handshake completes), but it is not a full two-way network partition.
