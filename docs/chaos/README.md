# **Chaos experiments: results**

Five fault-injection experiments against the local platform (kind, three worker nodes in three zones), run by `local/scripts/chaos-run.sh`: `broker-isolation` and `kafka-latency` on 2026-10-31, `flink-taskmanager-kill`, `zone-outage` (both with a standby Flink JobManager and a spare TaskManager) and `ingestion-handoff` on 2026-11-19. Experiment definitions: [**chaos/README.md**](../../chaos/README.md). Raw results: [**results/**](results/).

Each run sends 100 events per second through the ingestion service for 11 minutes, with a fresh range of event ids. The fault runs from minute 2 to minute 8. Afterwards the harness checks that **every one of those ids is in the Iceberg table exactly once**.

## **Results**

| **Experiment**             | **Fault**                                                                                                     | **Events sent** | **In Iceberg (missing / duplicates)** | **Peak consumer lag** | **Recovered after fault ends** | **Alerts fired (seconds after the fault started)**                                   | **Result** |
| -------------------------- | ------------------------------------------------------------------------------------------------------------- | --------------- | ------------------------------------- | --------------------- | ------------------------------ | ------------------------------------------------------------------------------------ | ---------- |
| **broker-isolation**       | One broker: 100% packet loss, 6 min                                                                           | 63,550          | 63,550 (0 / 0)                        | 800                   | 216 s                          | KafkaBrokerDown 222, KafkaNoActiveController 254, KafkaUnderReplicatedPartitions 409 | Passed     |
| **flink-taskmanager-kill** | The busy TaskManager killed under load (a spare was running)                                                  | 65,900          | 65,900 (0 / 0)                        | 2,750                 | 195 s                          | FlinkCheckpointFailures 49                                                           | Passed     |
| **kafka-latency**          | 400 ms delay, Flink to brokers, 6 min                                                                         | 66,000          | 66,000 (0 / 0)                        | 271                   | 225 s                          | none                                                                                 | Passed     |
| **zone-outage**            | Node `lakehouse-worker3` (zone-c) paused, 6 min; it ran the leading Flink JobManager and the busy TaskManager | 63,400          | 63,400 (0 / 0)                        | 5,346                 | 211 s                          | KafkaBrokerDown 173, KafkaUnderReplicatedPartitions 356                              | Passed     |
| **ingestion-handoff**      | An ingestion pod killed every 30 s, 6 min; every 20th reply discarded                                         | 63,750          | 63,750 (0 / 0)                        | 550                   | 206 s                          | none new (FlinkCheckpointFailures was already firing, see below)                     | Passed     |

How to read the table:

- **Recovery** is measured from the end of the fault to the moment the job is running and consumer lag is zero. The load continues for 3 minutes after the fault by design, so most of each figure is that tail; the backlog itself drained within seconds of the load ending.
- **Ingestion** never failed a request in the first four experiments. Load shedding and Kafka errors would have shown as retried batches: 2 in the broker isolation, 1 in the zone outage (its first attempt had not reached Kafka, so Flink dropped nothing), 0 otherwise. In `ingestion-handoff` 7 batches were retried because the pod kills cut their connections.
- **`IcebergCompactionOverdue`** was firing throughout and is left out of the table. It was real (the scheduled compaction is suspended locally) and unrelated to the faults; running the compaction cleared it.

## **What the experiments found**

| **Finding**                                                                                                                                                                                                                                                                                                                    | **Change**                                                                                                                                   |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------- |
| **The Flink JobManager had one replica: when its node failed, the job stopped until Kubernetes evicted the pod (5 minutes by default) and started it elsewhere (zone outage, 2026-11-19)**                                                                                                                                     | A standby JobManager in another zone (`jobManager.replicas: 2`, zone spread) took over in 13-17 s; see below                                 |
| **With the standby, the new leader's first TaskManager was scheduled onto the paused node, which Kubernetes had not yet marked NotReady. Flink waits 5 minutes by default for a TaskManager to register, so checkpoints still stalled for over 5 minutes**                                                                     | `resourcemanager.taskmanager-registration.timeout: 90s` (`taskManager.registrationTimeout`): Flink gives up on that pod and requests another |
| **The standby still waited for a new TaskManager pod after the zone outage (longest checkpoint gap 180 s)**                                                                                                                                                                                                                    | A spare TaskManager (`taskManager.spares: 1`), preferably in another zone: the gap fell to 100 s; see below                                  |
| **Alertmanager had one replica, on the failed node (zone outage): alert notifications would have stopped for the whole outage while Prometheus still detected it**                                                                                                                                                             | AWS runs two Alertmanagers spread across zones (`gitops/operators/kube-prometheus-stack/aws.yaml`)                                           |
| **A partition that never receives data reports lag -1 (the test traffic's 20 source keys never hash to partition 2), so lag sums never reached 0**                                                                                                                                                                             | Lag queries keep only `>= 0` (alert rule, dashboards, harness); a promtool test covers it                                                    |
| **Chaos Mesh's own webhook rejected requests: its chart generates a new CA on every render, and under Argo CD the certificate and the webhook's CA came from different renders**                                                                                                                                               | The operators ApplicationSet ignores those generated fields and keeps the first applied values                                               |
| **`pod-failure` is blocked by the admission policies (it rewrites a pod's image)**                                                                                                                                                                                                                                             | The experiments use faults that leave pod specs alone: packet loss, latency, pod kill, a paused node                                         |
| **On kind, the KRaft controller quorum re-elects about 190 times an hour, before and during the experiments. Docker Desktop's VM clock is corrected about every 34 seconds (297 seconds of drift an hour), and each step exceeds Kafka's 2-second controller fetch timeout. This is also why `KafkaNoActiveController` fired** | A property of this machine's VM, not the platform: data was never affected. Fix the host clock (see below)                                   |
| **Docker Desktop resumed a paused node by itself once, after 4 minutes 53 seconds**                                                                                                                                                                                                                                            | The harness now checks the node's real state during the fault and records an external resume                                                 |
| **The harness itself had Windows-specific bugs: `jq.exe` ends lines with `\r\n`, it cannot open MSYS paths, and a failing step in the cleanup trap skipped the rest**                                                                                                                                                          | `jq` wrapper in `local/scripts/lib.sh`, work files under `build/`, cleanup that always runs every step                                       |
| **400 ms of latency barely mattered at 100 events/s: peak lag 271, checkpoints under 1.4 s. The Kafka consumer pipelines fetches, so latency at this rate is absorbed**                                                                                                                                                        | None needed. A heavier load or more latency would be needed to push this past the alert thresholds                                           |

### **The clock problem on this machine**

Node boot times (`node_boot_time_seconds`) jumped about 106 times in an hour on every kind node. A clean restart of the VM usually stops the drift for a while:

```bash
wsl --shutdown
```

Then start Docker Desktop again; the kind nodes restart with it, and Argo CD and the Flink job recover by themselves (Flink from its latest checkpoint).

## **The zone outage with one JobManager (2026-11-19)**

The 2026-10-31 run passed with the Flink JobManager on a node that stayed up. A run on 2026-11-01 failed: the paused node held the JobManager (so `FlinkJobNotRunning`, then a forbidden alert, fired), and the load generator's port-forward was pinned to an ingestion pod on that node. The harness was fixed then: it forwards to an ingestion pod on another node and expects `FlinkJobNotRunning` when the JobManager is on the paused node. This run is the first since that fix, and the paused node held the JobManager again. It is the baseline for the standby below; raw result: [**results/zone-outage-single-jobmanager.json**](results/zone-outage-single-jobmanager.json).

- **Data:** 64,850 events, every one in Iceberg exactly once. One batch was retried; its first attempt had reached Kafka without the client getting a 200, and Flink dropped those 50 copies (`duplicatesDropped`).
- **Kafka and ingestion kept going on two zones.** `KafkaBrokerDown` fired at 175 s; producers never failed (one retried batch).
- **Flink did not.** The JobManager and a TaskManager were on the paused node, so the job stopped (`FlinkJobNotRunning` at 349 s). Kubernetes moved both pods only after its default 5-minute eviction for unreachable nodes, by which time the node was back. The job resumed from its checkpoint and drained a peak lag of 33,700 events.
- **`OpsAgentInvestigationsFailing`** fired because each investigation ended with an error before any model call (0 tokens, 0 tool calls): the agent has no working Claude access ([**ai-ops.md**](../ai-ops.md#not-verified)).
- **Already firing before the fault:** `FlinkCheckpointFailures`, left over from a Flink redeploy earlier that evening, and `InfoInhibitor`, kube-prometheus-stack's helper alert.

## **The zone outage with a standby JobManager (2026-11-19)**

The change: two JobManagers (`jobManager.replicas: 2` in the `flink-jobs` chart), spread so that they never share a zone. Kubernetes HA, already on, elects the leader; the standby runs no job until it wins the election. The harness now pauses the node of the *leading* JobManager (the one that reports job uptime) when that node qualifies, and with a standby elsewhere it forbids both `FlinkJobNotRunning` and `FlinkCheckpointsStalled`. In both runs the paused node also held the only TaskManager, so the job lost all of its processing at once.

| **Run**                             | **New leader after** | **Processing again after** | **Peak lag** | **Flink alerts**            | **In Iceberg (missing / duplicates)** |
| ----------------------------------- | -------------------- | -------------------------- | ------------ | --------------------------- | ------------------------------------- |
| **One JobManager** (baseline above) | no standby           | over 5 min                 | 33,700       | FlinkJobNotRunning 349      | 64,850 (0 / 0)                        |
| **Standby, run 1**                  | 17 s                 | 5 min 35 s                 | 31,950       | FlinkCheckpointsStalled 390 | 64,800 (0 / 0)                        |
| **Standby, run 2** (90 s timeout)   | 13 s                 | 2 min 13 s                 | 13,900       | none                        | 63,150 (0 / 0)                        |

Times count from the start of the fault. "New leader" is the standby's leadership log line; "processing again" is a replacement TaskManager registering with it (JobManager log), after which the job restored its latest checkpoint.

- **The standby took over within seconds both times.** Kubernetes still evicted the old pods after 5 minutes, but nothing waited for that any more: the new leader requested its own TaskManager.
- **Run 1 found the next bottleneck.** That request came 30 s into the outage, and Kubernetes scheduled the pod onto the paused node, which it marked NotReady only at 43 s. The pod never started, and by default Flink waits 5 minutes for a requested TaskManager to register before asking again. The job idled that long and `FlinkCheckpointsStalled` fired. The harness did not forbid that alert yet; it does now.
- **Run 2 added `resourcemanager.taskmanager-registration.timeout: 90s`.** The same thing happened (pod scheduled to the paused node at 28 s, node NotReady at 52 s), but Flink released the pod after 88 s and the next TaskManager registered on a live node 18 s later. Peak lag fell from 33,700 to 13,900.
- **Data:** every event exactly once in all three runs. Run 2 retried two batches whose first attempts had reached Kafka; Flink dropped those 100 copies.
- **Other alerts in run 2:** `KafkaBrokerDown` (expected) and `KafkaUnderReplicatedPartitions`; `OpsAgentAlertsDropped`, because the agent's hourly investigation budget ran out during the burst of alerts; `IcebergCompactionOverdue` again (see "How to read the table"). `FlinkCheckpointFailures` and `OpsAgentInvestigationsFailing` were already firing before the fault.
- **Cost:** one more JobManager pod, 0.5 CPU and 1 GiB by default (1 CPU and 2 GiB in prod).

## **The zone outage with a spare TaskManager (2026-11-19)**

The standby JobManager still had to wait for a new TaskManager pod. The change: one spare TaskManager (`taskManager.spares: 1`, Flink's `slotmanager.redundant-taskmanager-num`) that runs no tasks and keeps two free slots, with a soft zone spread (`ScheduleAnyway`) so that it usually runs in a different zone from the busy one. Flink starts a new spare whenever one is used. The harness now also records the longest gap between completed checkpoints (`max_checkpoint_gap_seconds`); nothing new reaches Iceberg during it. The gaps of the earlier runs were computed afterwards from Prometheus with the same query.

| **Zone outage run**                          | **Longest checkpoint gap** | **Peak lag** | **In Iceberg (missing / duplicates)** |
| -------------------------------------------- | -------------------------- | ------------ | ------------------------------------- |
| **One JobManager**                           | 495 s                      | 33,700       | 64,850 (0 / 0)                        |
| **Standby JobManager, run 1**                | 375 s                      | 31,950       | 64,800 (0 / 0)                        |
| **Standby JobManager, run 2** (90 s timeout) | 180 s                      | 13,900       | 63,150 (0 / 0)                        |
| **Standby JobManager and spare TaskManager** | 100 s                      | 5,346        | 63,400 (0 / 0)                        |

In every run the paused node held the leading JobManager and the TaskManager that ran the job. Gaps have 15 s resolution (the scrape interval) and include the normal 10 s between checkpoints. Raw results: [**zone-outage.json**](results/zone-outage.json) (spare), [**zone-outage-standby-jobmanager.json**](results/zone-outage-standby-jobmanager.json) (run 2), [**zone-outage-single-jobmanager.json**](results/zone-outage-single-jobmanager.json).

- **Timeline with the spare**, from the start of the fault: the standby became leader at 20 s, the spare on `lakehouse-worker2` registered with it at 23 s, and the job restored its latest checkpoint at 34 s and completed the next one at 85 s. Flink requested a new spare at 24 s; it registered at 46 s.
- **The last 50 s were not broken down.** Between the restore and the first completed checkpoint the tasks restarted and the Kafka partitions led by the paused broker had to move; the logs do not show how that time splits.
- **Setting up the case took one manual step.** Only `lakehouse-worker3` can be paused (the other nodes host Prometheus or the agent's tools), and after the TaskManager kill the busy TaskManager ran elsewhere. Its pod was deleted beforehand, so the job moved onto the TaskManager on `lakehouse-worker3`; that restart is why `FlinkJobRestarting` was already firing when the run began.
- **The TaskManager kill** ran with the spare too. The harness now labels a TaskManager that runs tasks as the target (`lakehouse.io/chaos-target`), because `mode: one` could otherwise kill the idle spare. The job moved to the spare and the longest checkpoint gap was 30 s, about normal. Kubernetes reports a killed pod at once and a replacement registered within 20 s, so for this fault the spare made no clear difference: peak lag 2,750, against 2,350 and 4,050 in the two earlier runs without one.
- **Data:** every event exactly once in both runs. The zone outage retried one batch whose first attempt had not reached Kafka (Flink dropped no copies).
- **Cost:** one more TaskManager pod, 1 CPU and 2 GiB by default (2 CPU and 8 GiB in prod, where the job uses 3 TaskManagers).

## **The ingestion handoff (2026-11-19)**

The question this answers: what happens to an event when the ingestion API fails before Kafka has it, or when Kafka has it but the reply never reaches the source? The source cannot tell these apart, so it retries with the same `event_id`, and the pipeline has to store the event exactly once either way ([**retry contract**](../../apps/ingestion-service/README.md#retry-contract)).

| **Measured**                        | **Value**   | **What it shows**                                                                                     |
| ----------------------------------- | ----------- | ----------------------------------------------------------------------------------------------------- |
| **Ingestion pods replaced**         | 12          | One kill every 30 s (`gracePeriod: 0`, no clean shutdown), counted from `kube_pod_info`               |
| **Batches retried after a cut**     | 7 (350 ids) | Requests that died with a pod; none of them left a copy in Kafka (see the next rows)                  |
| **Replies discarded by the client** | 63 (3,150)  | The server received the batch and Kafka stored it; the client closed the connection without reading   |
| **Copies dropped by Flink**         | 3,150       | `duplicatesDropped` counter: exactly 63 x 50, so every discarded reply did put a second copy in Kafka |
| **In Iceberg**                      | 63,750      | Every id once: 0 missing, 0 duplicates                                                                |

- **The lost-acknowledgement case was exercised for real:** the counter proves the retried copies reached Kafka, so the exactly-once result is not just a sign that no duplicates were ever created.
- **The pod kills only produced the other case.** The 7 cut batches added no copies, so each died before Kafka stored it (or before it reached the pod). No kill landed in the few milliseconds between the Kafka acknowledgement and the reply; the discarded replies stand in for that case.
- **`FlinkCheckpointFailures` was already firing** before the fault, left over from restarting the Flink job on its new image a few minutes earlier, and it resolved during the run. `InfoInhibitor` is kube-prometheus-stack's own helper alert. No forbidden alert fired.
- **Server-made ids are gone.** Before this run the service generated an `event_id` when the source sent none, and a lost reply then made a duplicate that no part of the pipeline could detect. The service now rejects events without one (400), so this experiment sends only client-chosen ids.

## **What these results do not show**

- **Single runs.** Each experiment ran once after the harness was fixed, so the timings are examples, not distributions.
- **The handoff drill's client is the test harness.** It retries correctly by construction. A real source that gives up, or retries after the 60-minute de-duplication window, can still lose or duplicate events; that part is a contract for the source, not something the platform can enforce.
- **A paused kind node is not a real availability zone.** All nodes share one host, one disk and one network, so the zone outage tests Kubernetes and Kafka behaviour, not real AWS failure modes.
- **The standby fix ran twice, on kind only.** On EKS a replacement TaskManager may first pull the 837 MB job image onto a node that never ran it, and that also counts toward the 90 s timeout; not measured. If a pod needs longer, Flink requests a new one each time (more delay, not data loss). A zone without room would leave the standby Pending (the spread is `DoNotSchedule`); not tested.
- **The spare was tested in one layout.** If only the busy TaskManager's node fails and the leader survives, Flink first has to notice: a paused node closes no connections, so that takes the heartbeat timeout (50 s by default). Not tested, because only one node can be paused here.
- **No AWS run.** Phase 8 was verified locally by choice; the AWS overlays render and validate, but the experiments did not run on EKS.
- **The broker isolation drops the broker's outgoing traffic only.** That makes it unreachable in practice (no TCP handshake completes), but it is not a full two-way network partition.
