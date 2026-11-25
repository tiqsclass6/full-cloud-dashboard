# **GitOps with Argo CD**

Argo CD deploys and continuously reconciles the whole platform from Git: the operators and the [**Helm charts**](../helm/README.md). Design reasons are in [**ADR 0006**](../docs/architecture-decisions/0006-helm-and-gitops.md).

```
root (Application, applied once)
 └─ gitops/argocd/
     ├─ project.yaml            AppProject "lakehouse"
     ├─ operators.yaml          ApplicationSet: clusters x {kube-prometheus-stack, strimzi, flink-operator, spark-operator}
     └─ platform.yaml           ApplicationSet: clusters -> helm/charts/platform with layered values
```

Both ApplicationSets select clusters labelled `lakehouse.io/managed=true` and use each cluster's `lakehouse.io/env` and `lakehouse.io/env-group` labels to choose values files. The local cluster is registered by `bootstrap/cluster-local.yaml` (env `local`, group `local`); in Phase 6 each EKS cluster is registered the same way with env `dev`, `staging` or `prod` and group `aws`.

| **Folder**                  | **Contents**                                                                                                                                                |
| --------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`bootstrap/`**            | Applied by `local/scripts/gitops-bootstrap.sh`, not from Git: Argo CD Helm values, the Gitea manifest, the local cluster registration, the root Application |
| **`argocd/`**               | What the root Application manages                                                                                                                           |
| **`operators/<operator>/`** | Values per environment group (`local.yaml`, `aws.yaml`) for the upstream operator charts                                                                    |

## **Where Argo CD pulls from**
On the local cluster the source is an **in-cluster Gitea** (`http://gitea-http.gitops.svc.cluster.local:3000/lakehouse/platform.git`). `make gitops-push` commits a snapshot of the working tree into a separate Git directory (`.gitops-mirror/`, git-ignored) and pushes it there. `.gitignore` is honoured, and the project's own repository (history, index, branches) is never touched. On AWS the source becomes a GitHub repository; only the URL changes.

## **Day-to-day**
```bash
make gitops-push          # publish changes; Argo CD syncs within about a minute
make gitops-status        # sync and health of every application
make local-ports          # Argo CD UI on http://localhost:8090 (prints the admin password), Gitea on :3300
```

To sync immediately after a push: `kubectl -n argocd annotate application platform-local argocd.argoproj.io/refresh=hard --overwrite`.

## **Manual changes and drift**
Automated sync runs with **self-heal**: a manual change to anything Git declares is reverted. Two exceptions are deliberate:

- **Ignored fields:** the FlinkDeployment's `spec.job.state` (stop or start the job) and `spec.restartNonce` (force a restart).
- **Pausing sync:** for experiments and destructive tests, pause automated sync on the platform application. The scripts in `local/scripts` do this themselves.

```bash
source local/scripts/lib.sh
gitops_pause      # manual changes now stick; the application shows OutOfSync
gitops_resume     # Argo CD restores what Git declares
```

## **What was verified (local kind cluster, 2026-10-29)**
| **Check**                                       | **Result**                                                                                                                                                                                                                                                                                                                               |
| ----------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Bootstrap on the running Phase 1-4 cluster**  | All 6 applications Synced and Healthy; existing workloads adopted in place, Kafka and table data intact                                                                                                                                                                                                                                  |
| **Adoption side effects**                       | Every platform pod restarted once for the new labels; Kafka rolled its brokers one at a time; the Flink job, which had been FAILED since the cluster restart, came back RUNNING through the new `restartFailedJob` setting                                                                                                               |
| **Change through Git**                          | Adding the `smoke.test` topic to `values/local.yaml` and pushing created the topic; Argo CD reported the pushed revision                                                                                                                                                                                                                 |
| **Self-heal**                                   | A manual scale of the ingestion Deployment to 1 replica was reverted in about 1 s                                                                                                                                                                                                                                                        |
| **Pause and resume**                            | While paused the manual change stayed for 45 s and the app showed OutOfSync; on resume it was reverted in about 2 s                                                                                                                                                                                                                      |
| **Charts**                                      | `bash local/scripts/helm-check.sh --server`: lint, render, kubeconform (strict, with CRD schemas) and a server-side dry run passed for local, dev, staging and prod                                                                                                                                                                      |
| **End to end after adoption**                   | `make local-smoke` 15/15; events posted to the ingestion service reached Kafka and Flink (consumer lag 0)                                                                                                                                                                                                                                |
| **`make local-up` from scratch, first run**     | Exit 0 in 37.7 min; all 6 apps Synced and Healthy. **But** the Flink JobManager crash-looped 5 times (about 7 min) before running: it started before the `checkpoints` bucket existed, which its HA store needs. Argo CD still reported the platform Healthy, and the smoke test did not check Flink. See "Fresh-install ordering" below |
| **`make local-up` from scratch, after the fix** | Exit 0 in 25.6 min; all 6 apps Synced and Healthy; Flink RUNNING with **0** JobManager restarts; pods created in wave order; `make local-smoke` 17/17 (now including two Flink checks)                                                                                                                                                   |
| **Scripts re-run under GitOps**                 | Exactly-once test passed (24,000 / 24,000, no duplicates); schema-evolution demo passed; perf run completed (30,000 events/s, 0 errors). After each, Argo CD restored the deployments from Git (details in docs/flink-iceberg.md and docs/performance)                                                                                   |

## **Incident found during this phase: 500,000 duplicate rows**
While verifying the adoption, an independent check of the events table (PyIceberg) found **4,001,502 rows but only 3,501,502 distinct event ids**. Kafka held 3,501,602 messages (the 3.5 million events, 100 plain-text smoke messages that Flink correctly skipped, and 2 test events), so the extra rows were not real input. The table had verified clean at the end of Phase 4.

**Cause.**
- The Flink job had been FAILED since the cluster restart: it started while the catalog was still coming up.
- Adoption changed the FlinkDeployment spec, which makes the operator upgrade the job. A failed job cannot take a fresh savepoint, and without Flink HA the operator had no record of the latest checkpoint.
- So it restored the last savepoint it knew, taken a day earlier, and replayed 500,000 events that were already committed. The JobManager log shows the job starting from that savepoint.
- The de-duplication state could not catch the replay, because its 60-minute TTL had long expired.
- The `restartFailedJob` setting added in this phase has the same dependency.

**Fix.**
1. Flink Kubernetes HA is now on in every environment, including local. With HA the operator recovers a job that is not running from its latest checkpoint.
2. The `flink-jobs` chart refuses to render `restartFailedJob` without HA, so the unsafe combination cannot be deployed again.
3. A new, opt-in `dedupe` repair step in the Spark maintenance job, with three unit tests, and a runbook script `local/scripts/repair-duplicates.sh`. The script suspends the job, deduplicates and compacts, resumes, then verifies.

**Repair.** The runbook removed the 500,000 duplicates. Afterwards PyIceberg read 3,501,502 rows, all distinct, which is exactly Kafka's real events.

**Regression test.** `local/scripts/flink-recovery-test.sh` sends 20,000 events, then scales the catalog to 0 and deletes the JobManager pod while 20,000 more arrive, then restores the catalog. Result: **passed**.
- The job resumed from its latest checkpoint (the log shows checkpoint 391, not a savepoint).
- The table grew by exactly the 20,000 new Kafka messages, with every id distinct.
- Limit: during the outage the job showed RECONCILING (the JobManager restarting under HA), not FAILED, so the exact "upgrade a FAILED job" path of the incident was not reproduced. HA and the chart guard address that path, but it is not directly tested.

**Lesson.** Exactly-once holds only when a job resumes from its latest checkpoint. Any path that can fall back to an older savepoint (manual restores, operator fallbacks without HA) replays data, and short-lived de-duplication state cannot be relied on to catch it.

## **Fresh-install ordering (found on the first from-scratch run)**
On an empty cluster Argo CD created everything in the platform chart at once. The Flink JobManager, which with HA keeps its job-result store in the `checkpoints` bucket, started before the bucket-creation Job had run (that Job was a post-sync hook, so it ran last). The JobManager failed with `NoSuchBucket` and recovered only through crash-loop restarts.

**Fix.** Argo CD sync waves in the charts:

| **Wave** | **Resources**                                                                |
| -------- | ---------------------------------------------------------------------------- |
| **-4**   | namespaces, Kyverno policies (healthy once Kyverno enforces them)            |
| **-3**   | secrets, Postgres, SeaweedFS                                                 |
| **-2**   | Iceberg REST catalog, toolbox                                                |
| **-1**   | bucket creation (now an Argo CD `Sync`-phase hook, so it runs in wave order) |
| **0**    | everything else (Kafka, registry, ingestion, Flink, Spark schedules)         |

Argo CD waits for each wave to be healthy before starting the next. The smoke test also gained two Flink checks (job RUNNING, checkpoints completing), because Argo CD's health for the FlinkDeployment did not flag the crash loop. On the second from-scratch run the JobManager started once, with no restarts.

**Kyverno policies (added 2026-11-25).** On the fifth AWS run the platform synced before Kyverno was installed: the Deployments came first, the policies 18 minutes later. Kyverno then pinned image digests into the Deployment templates and started a second rollout, which stuck on full nodes ([**aws.md**](../docs/aws.md#what-the-run-found-here-fifth-run)). The policies are now in wave -4. A missing Kyverno CRD fails that wave, so Argo CD retries the sync and creates no workload until Kyverno is installed. A health check in `gitops/bootstrap/argocd-values.yaml` keeps each policy `Progressing` until Kyverno reports it ready (webhook configured), so the next wave also waits for enforcement.

### **Not verified yet**
- **The AWS overlays:** rendered and validated only, never deployed (Phase 6).
- **The incident's exact path** (upgrading a FAILED job) is covered by HA and the chart guard, but the recovery test reached it only through JobManager failover; see the incident section.
- **Single runs:** each test above was run once (twice for the fresh install), on one laptop.
