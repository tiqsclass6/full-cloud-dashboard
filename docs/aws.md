# **AWS environment (ephemeral EKS)**

The same platform as the local kind cluster, on AWS in us-east-1: created for a demo, verified, and destroyed in the same session. Design decisions are in [**ADR 0007**](architecture-decisions/0007-ephemeral-eks-environment.md).

## **What Terraform creates (`terraform/envs/dev`)**

| **Area**       | **Resources**                                                                                                                                                                                                                                                                                                       |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Network**    | VPC over 3 AZs, private subnets for nodes and RDS, public subnets for one NAT gateway, S3 gateway endpoint                                                                                                                                                                                                          |
| **Kubernetes** | EKS 1.37, one spot node group per AZ (xlarge, IMDSv2 hop limit 1), add-ons: VPC CNI, CoreDNS, kube-proxy, EBS CSI, metrics-server                                                                                                                                                                                   |
| **Identity**   | IRSA roles: EBS CSI driver, Flink jobs, Spark jobs (lakehouse buckets, KMS key, `lakehouse` Glue database only)                                                                                                                                                                                                     |
| **Data**       | KMS key; S3 buckets for the warehouse, checkpoints and Athena results (SSE-KMS, TLS only, no public access); Glue database                                                                                                                                                                                          |
| **Query**      | Athena workgroup (engine v3, results encrypted, 1 GiB scan limit per query)                                                                                                                                                                                                                                         |
| **Database**   | RDS Postgres 17, db.t4g.micro, password managed by RDS in Secrets Manager                                                                                                                                                                                                                                           |
| **Images**     | ECR repositories for the seven platform images (scan on push, KMS-encrypted)                                                                                                                                                                                                                                        |
| **Ops agent**  | AgentCore Gateway (MCP, SigV4 callers); AgentCore Runtime for the agent (second apply); IRSA for ops-agent (model access, InvokeAgentRuntime), Hermes (inference profile only) and the AWS Load Balancer Controller; a Secrets Manager slot for the MCP endpoint's CA certificate ([**docs/ai-ops.md**](ai-ops.md)) |
| **Security**   | KMS image-signing key; IRSA for External Secrets and Kyverno; Grafana admin secret (write-only); TLS-only RDS parameter group; VPC CNI network policy enforcement; AWS Config recording and 9 rules while the environment exists                                                                                    |

Not in Terraform: the state bucket and budget alerts (`terraform/bootstrap`, applied once and kept), and everything inside the cluster (Argo CD deploys it from Git).

## **Run it**

Prerequisites: the bootstrap stack applied, AWS credentials for the account, Docker, `terraform`, `kubectl`, `helm`, `jq`. Copy `terraform/envs/dev/terraform.tfvars.example` to `terraform.tfvars` and put your public address in `api_allowed_cidrs`.

```bash
make aws-init
```

```bash
make aws-plan
```

```bash
make aws-apply
```

```bash
make aws-up
```

```bash
make aws-agentcore
```

```bash
make aws-smoke
```

```bash
make aws-security-test
```

```bash
make aws-ai-test
```

```bash
make aws-down
```

| **Step**                | **What it does**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`aws-init`**          | `terraform init` with the state bucket name derived from the account id                                                                                                                                                                                                                                                                                                                                                                                                                            |
| **`aws-plan`**          | Saves a plan to `tfplan`; review it before applying                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| **`aws-apply`**         | Applies exactly the saved plan                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| **`aws-up`**            | kubeconfig (context `lakehouse-dev`), generated values, security gate (`security-scan.sh`), images pushed to ECR and signed with the KMS key (Cosign), gp3 StorageClass, then the same GitOps bootstrap as kind, plus the `ai-ops` Secrets (`aws-ai-secrets.sh`) once Argo CD has created the namespace. Credentials come from Secrets Manager through External Secrets                                                                                                                            |
| **`aws-agentcore`**     | The ops agent's AWS path (`local/scripts/aws-agentcore.sh`): TLS certificate for the MCP endpoint from a throwaway CA (its key is deleted at once; made by `aws-ai-secrets.sh`, which `aws-up` already ran), the internal NLB, an API key provider with the MCP token, and the Gateway target through VPC Lattice. Then a second `aws-plan`/`aws-apply` with `agentcore_runtime=true` creates the Runtime from the arm64 image, and `aws-values.sh` + `gitops-push` give ops-agent its ARN         |
| **`aws-smoke`**         | Argo CD apps healthy, brokers in 3 zones, Flink running with IRSA and checkpoints on S3, then events through the ingestion service and an Athena count of the Iceberg table                                                                                                                                                                                                                                                                                                                        |
| **`aws-security-test`** | Signed image admitted and unsigned image refused, External Secrets synced, RDS TLS enforced, CloudTrail/Config/Access Analyzer working, then the Kafka, network and admission suites on EKS ([**SECURITY.md**](../SECURITY.md))                                                                                                                                                                                                                                                                    |
| **`aws-ai-test`**       | The ops agent on AWS hop by hop: workloads from signed ECR images, audit store on RDS, internal NLB, Gateway target READY and tools/list through it, Runtime selftest, an alert end to end (ops-agent, Runtime, ops-control), Hermes                                                                                                                                                                                                                                                               |
| **`aws-down`**          | Stops Argo CD, deletes the platform namespaces and waits for every EBS volume to be released, `terraform destroy`, deletes this environment's CloudWatch log groups and the older local Docker images (it keeps the newest build of each app image, so each live run replaces the last; `:test` tags and untagged builds go; `PURGE_DEV_IMAGES=1` removes the newest build too), then checks for leftovers (EBS volumes, load balancers, VPC, cluster, RDS, log groups, stale local images, state) |

Images are built for `linux/amd64` on any build machine (`IMAGE_PLATFORM` in `local/scripts/lib.sh`), to match the `AL2023_x86_64` nodes and kind on an x86 host. The only exception is the agent image for AgentCore Runtime, which `aws-images.sh` builds separately for `linux/arm64`. `DOCKER_BUILD_FLAGS="--pull --no-cache"` forces a clean rebuild in the build scripts.

## **Account-specific values**

The account id, bucket names, RDS endpoint and role ARNs are not in the repository. `local/scripts/aws-values.sh` writes them from Terraform outputs into git-ignored files, which the ApplicationSets layer last:

| **File**                                                   | **Contents**                                            |
| ---------------------------------------------------------- | ------------------------------------------------------- |
| **`helm/values/generated/<env>.yaml`**                     | ECR registry, Iceberg warehouse, Flink storage, RDS URL |
| **`gitops/operators/flink-operator/generated-<env>.yaml`** | IRSA role for the Flink job service account             |
| **`gitops/operators/spark-operator/generated-<env>.yaml`** | IRSA role for the Spark job service account             |

`gitops-push.sh` force-adds them to the snapshot it pushes to the cluster's own Gitea, so they reach Argo CD without being committed. On EKS, publish changes with `make aws-gitops-push`: the plain `make gitops-push` targets the kind cluster and fails with `Failed to connect to localhost:3300` when kind is stopped.

## **Verified run (2026-10-29, dev)**

The Phase 7 run (2026-10-30, security controls on) is reported in [**SECURITY.md**](../SECURITY.md).

A single run, from an empty environment to destroyed. The environment existed for about an hour (the EKS cluster for about 35 minutes); at roughly $0.40 per hour that is under $1, plus a few cents of S3, KMS and Athena usage.

| **Step**                 | **Result**                                                                                                                                                                                                                                                               |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **`terraform apply`**    | 113 resources. The first attempt created 88 in 8 minutes, then stopped at the EKS cluster on an invalid allowlist entry (see below); the second added the remaining 25 (cluster, node groups, add-ons, IRSA) in 13m10s                                                   |
| **`aws-up`**             | 8m19s: images rebuilt and pushed, Secrets, Argo CD, Gitea, then all six Argo CD applications Synced and Healthy                                                                                                                                                          |
| **Nodes**                | 3 spot nodes, one per zone (us-east-1a, b, c; m6a.xlarge, m6a.xlarge, m6i.xlarge)                                                                                                                                                                                        |
| **`aws-smoke`**          | 13/13 checks passed                                                                                                                                                                                                                                                      |
| **Kafka**                | 3 brokers in 3 zones, `events.raw` RF=3, volumes on gp3 (brokers 20Gi, controllers 10Gi)                                                                                                                                                                                 |
| **Flink**                | Job RUNNING, IRSA role injected, checkpoints and HA metadata on S3                                                                                                                                                                                                       |
| **Pipeline**             | 5,000 events + 500 duplicate sends through the ingestion service; Athena counted 5,000 rows, 5,000 distinct                                                                                                                                                              |
| **Spark (one-off runs)** | Hourly aggregate wrote `events_hourly` (Athena sum: 5,000); maintenance rewrote the manifests (2 to 1) and committed through Glue                                                                                                                                        |
| **Restarts**             | Only the two ingestion pods, twice each at start-up while Kafka was not yet reachable                                                                                                                                                                                    |
| **`aws-down`**           | 21m17s: all PersistentVolumes released through Kubernetes, 113 resources destroyed, leftover check clean (EBS volumes, ENIs, Elastic IPs, NAT gateways, load balancers, VPC, cluster, RDS, state). The KMS key is pending deletion (7 days); the bootstrap stack remains |

### **What the run found**

- **An empty public-IP lookup produced an invalid CIDR.** `curl` in Git Bash failed its TLS handshake (exit 35) and wrote nothing, so `api_allowed_cidrs` became `["/32"]`. `terraform plan` accepted it and EKS rejected it eight minutes into the apply. The variable now has a validation rule, so a malformed CIDR fails at plan time.
- **No metrics-server on EKS.** The ingestion service's HPA needs resource metrics; without them Argo CD would report the application Degraded. Added the EKS `metrics-server` add-on.
- **Spark executors had no service account.** Only the driver used the IRSA service account; executors, which write data files, would have run without AWS credentials. The chart now sets it on executors too.
- **Flink's S3 plugin and IRSA.** The plugin bundles Hadoop 3.3.4 with AWS SDK v1, whose default credential chain omits web identity. The AWS overlay names `WebIdentityTokenCredentialsProvider` explicitly.

### **Not verified**

- Staging and prod: their overlays render and validate, but only dev was applied.
- Behaviour over time (scheduled runs on their own cron, spot interruptions, node replacement into the same zone).
- Grafana and Prometheus dashboards on EKS were not inspected beyond the applications being healthy.

## **Second run (2026-11-03, dev, with the ops agent on AgentCore)**

A single run, with the security controls on and the optional AgentCore path (Gateway and Runtime). It needed several retries, listed below; the figures are from the runs that passed.

| **Step**                | **Result**                                                                                                                                                                                                                                                                                          |
| ----------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`terraform apply`**   | One run failed with two errors, then a reapply completed (38 resources): the AgentCore Gateway was refused (`kms:GenerateDataKey` not allowed for its role) and a leftover EKS log group blocked the cluster's own (see below)                                                                      |
| **`aws-up`**            | Images built, tested, scanned, pushed and signed (seven, plus the arm64 agent image); Argo CD, Gitea and 13 of 14 applications Healthy; `platform-dev` Degraded until the `ai-ops` Secrets existed, so the 30-minute wait timed out (see below)                                                     |
| **Nodes**               | Spot capacity was replaced during the run (two of the three original nodes were gone within about two hours); the platform stayed Healthy                                                                                                                                                           |
| **`aws-smoke`**         | Passed on the third attempt: 14 applications Synced and Healthy, 524 checkpoints, 5,000 events and 500 duplicate sends, Athena 5,000 rows and 5,000 distinct, 53 Iceberg snapshots                                                                                                                  |
| **`aws-security-test`** | Passed: signed image admitted, unsigned refused, secrets synced, RDS TLS enforced, CloudTrail, Config and Access Analyzer active; Kafka (10), network policy (9) and admission (15) suites                                                                                                          |
| **Runtime apply**       | One resource, 4m30s                                                                                                                                                                                                                                                                                 |
| **`aws-ai-test`**       | Workloads, Gateway (13 tools) and Runtime self-test (13 tools, `get_kafka_status` in 1.49 s) passed; the end-to-end alert was admitted, forwarded to the Runtime and recorded, but the model call returned `404` (outcome `error`)                                                                  |
| **`aws-down`**          | `terraform destroy` stopped on the security group and two subnets: two AgentCore `agentic_ai` interfaces were still attached, as on 2026-11-01. No EKS cluster, RDS instance, NAT gateway, Elastic IP, volume or load balancer was left at that point. The result of the retry is not recorded here |

### **What the run found here**

- **The Gateway still failed with `depends_on` in place.** `CreateGateway` was refused for `kms:GenerateDataKey` although its role policy existed: IAM changes take a few seconds to reach AgentCore. A `time_sleep` of 30 seconds after the policy now sits in front of the Gateway.
- **The ops agent could not start on a base `aws-up`.** The `ai-ops` Secrets (`ai-ops-internal`, `mcp-tls`) were only created by the optional `aws-agentcore` step, so four pods stayed in `CreateContainerConfigError` and the platform never turned Healthy. `aws-up` now creates them while Argo CD syncs (`aws-ai-secrets.sh`).
- **Windows reserved the smoke test's ports.** A reserved range (8002-8101) made `kubectl port-forward` fail on 8081 and 8082, hidden by the script, so the pipeline step got empty answers. `fwd` now prints the error; the fix (reserve the ports) is in the runbook's Troubleshooting.
- **EKS recreates its log group after a teardown.** The empty `/aws/eks/lakehouse-dev/cluster` came back after `terraform destroy` and the next apply failed with `ResourceAlreadyExistsException`. `aws-down.sh` now deletes it, and the AgentCore Runtime log groups.
- **A second `aws-down` died on an empty region.** After the first, partly failed `terraform destroy` the state has no outputs, and `terraform output -raw region` then prints nothing and exits 0, so the `|| echo us-east-1` fallback never ran and every later AWS call failed with `Invalid endpoint`. `aws-down.sh` and `aws-agentcore.sh down` now fall back to `us-east-1` (and to `lakehouse-<env>`) when the outputs are empty; checked against the real output-less state, not through a full rerun.
- **The AgentCore arm64 image needs the QEMU emulator** on an x86 machine; `aws-up.sh` now checks and registers it before building.
- **AWS Config reported `s3-bucket-ssl-requests-only` NON_COMPLIANT** for eight buckets. None is a `lakehouse-dev-*` bucket: they are other buckets in the account, not managed here.

### **Not verified here**

- **Anything the model does.** The end-to-end alert ended in `outcome: error` (`404` for the model): the account has no access to the Claude models in Bedrock (see [**ai-ops.md**](ai-ops.md#not-verified)).
- **The fixes made after this run:** the `time_sleep`, the `ai-ops` Secrets in `aws-up`, the log-group and image cleanup in `aws-down`, and the Docker preflight were written after the failures they address and have not been run end to end.
- **A clean teardown.** The leftover-check result of this run's `aws-down` is not recorded.

## **Third run (2026-11-13, dev, base path without AgentCore)**

A single run of the lab's Part 9, without the optional AgentCore Runtime (step 6): the account still has no Bedrock access to the Claude models. Every step passed on its first attempt. The environment existed for about two hours, mostly for screenshots 19-21.

| **Step**                | **Result**                                                                                                                                                                                                   |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **`terraform apply`**   | 162 resources, no errors; the EKS cluster took 11m20s and RDS 9m6s. The AgentCore Gateway was created after the 30-second IAM wait                                                                           |
| **`aws-up`**            | Docker preflight passed (arm64 emulator present); seven images and the arm64 agent image built, scanned, pushed and signed; all 14 Argo CD applications Synced and Healthy                                   |
| **Nodes**               | 3 spot nodes, one per zone (m6a.xlarge, m5a.xlarge, m6i.xlarge)                                                                                                                                              |
| **`aws-smoke`**         | Passed: brokers in 3 zones, `events.raw` RF=3, Flink RUNNING with 101 checkpoints, 5,000 events and 500 duplicate sends, Athena 5,000 rows and 5,000 distinct, 12 Iceberg snapshots                          |
| **`aws-security-test`** | Passed, the same checks as the second run; Config: 8 rules COMPLIANT, `eks-endpoint-no-public-access` NON_COMPLIANT (the known gap in the control mapping)                                                   |
| **`aws-down`**          | 162 resources destroyed; VPC, cluster, RDS, log groups, load balancers and state clean. Six EBS volumes were still `deleting` at the leftover check (see below); a rerun the next morning found no leftovers |

### **What the run found here (Third run)**

- **The leftover check counted volumes AWS was still deleting.** All PersistentVolumes were released and the EBS CSI driver had issued the deletes, but the six volumes (Gitea, Tempo, four Kafka) were still in the `deleting` state when the check ran, so `aws-down` reported FAIL. They were encrypted with the AWS-managed `aws/ebs` key, not the project key. The deletes were issued at 22:56; the first volume was gone after about 50 minutes, three after 70 minutes, and the last three after more than 80 minutes (the watch stopped then; all were gone the next morning). Waiting that long in the script is not practical: the check now waits 5 minutes, then reports volumes still in `deleting` as pending (not FAIL) and asks for a rerun later; any other state still fails.
- **The fixes made after the second run held:** the Gateway's IAM wait, the `ai-ops` Secrets created by `aws-up` (the platform turned Healthy without the AgentCore step), the Docker preflight, and the log-group cleanup in `aws-down`.
- **`ports.sh` stopped on EKS.** It forwarded Langfuse, the Iceberg REST catalog and the S3 stand-in, which run only on kind. It now skips a service the cluster does not have.

### **Not verified here (Third run)**

- **AgentCore and anything the model does** (step 6 skipped).
- **The new volume check** in `aws-down.sh`: written after the rerun and checked only on sample input, not through a teardown.

## **Fourth run (2026-11-24, dev, data integrity on EKS)**

A single run to check the integrity work since PR #9 (lineage, the reconciliation, replays, the Integrity dashboard) on the Glue catalog, without AgentCore Runtime (still no Bedrock access to Claude) and without `aws-security-test` or `aws-ai-test`. The environment existed from 23:14 to 02:55 UTC (3 h 41 min).

| **Step**              | **Result**                                                                                                                                |
| --------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| **`terraform apply`** | 165 resources in three applies: the first two stopped at the AgentCore Gateway (`kms:GenerateDataKey` denied)                             |
| **`aws-up`**          | First attempt stopped at the security gate (Spark image); the rerun with `DOCKER_BUILD_FLAGS=--no-cache` passed                           |
| **`aws-smoke`**       | Passed: brokers in 3 zones, RF=3, Flink RUNNING with IRSA, 5,000 events and 500 duplicate sends, Athena 5,000 rows                        |
| **Integrity checks**  | Passed except one case (a lost first offset, fixed since by ADR 0013); [**integrity results**](integrity/README.md#on-eks-2026-11-24-dev) |
| **Dashboards**        | After the fixes below, every query of Integrity, Overview and Kafka returned data                                                         |
| **`aws-down`**        | Passed in 23 minutes: 165 destroyed, no leftovers (no volume left in `deleting` this time)                                                |

### **What the run found here (Fourth run)**

- **The Gateway could not use the KMS key.** The policy allowed the key only with `kms:ViaService` (added 2026-11-14, first used now). CloudTrail shows AgentCore calling KMS itself with the role (source `bedrock-agentcore.amazonaws.com`), so the condition never matches; a longer IAM wait did not help. The condition is gone; the trust policy still limits the role to this account's Gateway.
- **A cached image layer failed the security gate.** The Spark image runs `apt-get upgrade`, but Docker reused the layer from an earlier build, before Ubuntu fixed `linux-libc-dev` (73 HIGH/CRITICAL findings). `DOCKER_BUILD_FLAGS=--no-cache` rebuilt it clean. A rebuild before an AWS run needs `--no-cache` when the base image has not changed.
- **No Spark job could start on dev.** The jobs requested 3 CPUs each, and the nodes had at most 635 millicores left to request; with no autoscaler the executors stayed `Pending`, the hourly schedules included. dev now requests 250 m per Spark pod (`helm/values/dev.yaml`).
- **Argo CD had no metrics on a fresh cluster.** The chart creates its ServiceMonitors only if the CRD exists, and on a fresh cluster the CRD comes later with kube-prometheus-stack. The Overview's Argo CD tiles were empty, and `ArgoCDAppOutOfSync` and `ArgoCDAppDegraded` had no data. This follows from the chart for any fresh install; it was seen on EKS (kind has the metrics, presumably from a later upgrade of Argo CD). `gitops-bootstrap.sh` now upgrades Argo CD once more after kube-prometheus-stack is healthy.
- **`KafkaBrokerDown` counted controllers as brokers.** AWS runs three dedicated controllers, scraped by the same job, so the count was 6 and one lost broker left 5 (the alert fires below 3). The rule and the dashboards now leave out the `controllers` pool; a promtool case covers the AWS layout.
- **"Events in, published" showed no data** after the HPA removed the pod that had taken the traffic: the counter exists only after the first event. It now falls back to 0, like the other series.
- **The replay pod copied the JobManager's IRSA variables.** EKS injects them again, with the token volume, so the pod now leaves them out (fixed before the run; replays worked on EKS).

### **Not verified here (Fourth run)**

- **AgentCore beyond creating the Gateway**, the model, Hermes, and the security and AI suites (not run).
- **The second Argo CD pass in `gitops-bootstrap.sh`.** The same command was run by hand on this cluster; a fresh bootstrap has not run it.
- **Spark jobs on dev under real load** with 250 m requests: the run had 5,000 rows.
