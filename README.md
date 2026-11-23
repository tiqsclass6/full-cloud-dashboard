# **Streaming Data Lakehouse Platform on Kubernetes**

![Streaming lakehouse platform](docs/diagrams/banner.jpg)

![Kubernetes](https://img.shields.io/badge/Kubernetes-kind%20%7C%20EKS%201.37-326CE5?logo=kubernetes&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-1.10%2B-844FBA?logo=terraform&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-EKS%20%7C%20S3%20%7C%20Glue%20%7C%20Athena-FF9900?logo=amazonwebservices&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Kafka-4.3-231F20?logo=apachekafka&logoColor=white)
![Apache Flink](https://img.shields.io/badge/Flink-2.1.3-E6526F?logo=apacheflink&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Spark-4.1.4-E25A1C?logo=apachespark&logoColor=white)
![Apache Iceberg](https://img.shields.io/badge/Iceberg-1.12-4E8EE9)
![Argo CD](https://img.shields.io/badge/Argo%20CD-3.5.6-EF7B4D?logo=argo&logoColor=white)
![Java](https://img.shields.io/badge/Java-21%20%7C%2017-007396?logo=openjdk&logoColor=white)
[![M8ven Score](https://m8ven.ai/badge/mcp/tiqsclass6-full-cloud-dashboard-1qz06h?v=2ededef2381af07f1761951bbdfc8fee)](https://m8ven.ai/mcp/tiqsclass6-full-cloud-dashboard-1qz06h?s=readme)
![Python](https://img.shields.io/badge/Python-3.14-3776AB?logo=python&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green)

---

## **Contents**

1. [**Overview**](#overview)
2. [**Requirements**](#requirements)
3. [**Project structure**](#project-structure)
4. [**Steps to complete the lab**](#steps-to-complete-the-lab)
5. [**Artifacts and screenshots**](#artifacts-and-screenshots)
6. [**Teardown**](#teardown)
7. [**Lessons learned**](#lessons-learned)
8. [**References**](#references)
9. [**Troubleshooting**](#troubleshooting)
10. [**Author and contributors**](#author-and-contributors)

---

## **Overview**

A Kubernetes-native streaming lakehouse, built to production habits and tested end to end: a Java ingestion service writes Avro events to Kafka, Flink lands them in Apache Iceberg **exactly once**, Spark keeps the tables healthy, and Argo CD delivers all of it from Git. It runs **locally on kind** and as an **ephemeral Amazon EKS environment** provisioned with Terraform (apply, verify, destroy). An **agentic ops assistant** triages alerts through read-only MCP tools and can change the platform only after a person approves.

It started as an incomplete serverless AWS dashboard scaffold and was rebuilt, phase by phase, around the skills of a senior staff data-platform engineering role ([**how the requirements map to this repo**](#skills-map)).

---

## **Recruiter-friendly summary**

Built a Terraform-provisioned, Kubernetes-native streaming data lakehouse that runs on a local kind cluster and as a short-lived Amazon EKS environment. A Java service streams events through Kafka and Flink into Apache Iceberg tables exactly once, Spark keeps the tables fast, and Argo CD deploys everything from Git. The AWS environment uses EKS, S3, Glue, Athena, RDS, KMS, ECR, IAM, Secrets Manager, CloudTrail and AWS Config. An AI ops assistant (Claude on Amazon Bedrock, with AgentCore) triages alerts through read-only tools and needs a human's approval before it changes anything.

### **Real-world example**

A company may want to stream app or device events into a data lake that analysts query with SQL, without losing or double-counting records when servers fail. This project builds that pipeline and proves it: 24,000 events sent with 3,400 duplicates, a killed worker and a restore from a savepoint still produced exactly 24,000 rows, and chaos tests with 323,300 events lost and duplicated none, including ingestion pods killed mid-request and replies thrown away so the client had to retry. When an alert fires, the assistant gathers metrics, logs and runbooks and proposes one fix from a fixed list, and nothing runs until a person approves it. Its guardrails are tested; live diagnoses wait on Bedrock model access for the AWS account.

![Platform architecture](docs/diagrams/architecture.png)

> **Status (2026-11-14): all ten phases built.** The lab was run with the screenshots below: Parts 1-8 of the [**runbook**](docs/lab/RUNBOOK.md) on the local kind cluster, and Part 9 on AWS (2026-11-13). The AWS environment was applied, verified and torn down in five recorded runs (2026-10-29, 10-30, 11-01, 11-03, 11-13; [**aws.md**](docs/aws.md), [**SECURITY.md**](SECURITY.md), [**ai-ops.md**](docs/ai-ops.md)); the latest teardown passed its leftover check on 2026-11-14, and no AWS environment exists now. **One gap:** the AWS account has no Amazon Bedrock access to Claude models yet, so the agent's guardrails, tools, approvals and AWS path are verified, but no live diagnosis has been produced and the agent eval suite has no results ([**details**](docs/ai-ops.md#not-verified)).

### **Results**

Numbers from recorded runs. Each was run once unless stated; the linked pages say what each test does and does not prove.

| **Area**                     | **Result**                                                                                                                                                                                                                      | **Details**                                         |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------- |
| **Exactly once**             | 24,000 events with 3,400 duplicate sends and re-sends, a TaskManager kill and a savepoint restore: **24,000 rows, 24,000 distinct**                                                                                             | [**flink-iceberg.md**](docs/flink-iceberg.md)       |
| **Chaos**                    | Broker isolation, TaskManager kill, 400 ms Kafka latency, a paused node, ingestion pods killed with replies lost: **323,300 events, 0 lost, 0 duplicated**                                                                      | [**chaos/README.md**](docs/chaos/README.md)         |
| **Iceberg maintenance**      | 70 small files to 1, 56 snapshots to 1, median full scan 1.68 s to 0.48 s; compaction under 5,000 events/s with **3,501,500 = 3,501,500**                                                                                       | [**spark-iceberg.md**](docs/spark-iceberg.md)       |
| **JVM performance**          | 30,000 events/s, 0 errors, p99 23 ms; an Avro encoder change cut allocation 19%; G1 vs generational ZGC compared with JFR                                                                                                       | [**perf/README.md**](docs/performance/README.md)    |
| **Data integrity**           | 3,000 events, 300 retried copies, 3 non-Avro records: **3,303 Kafka offsets, each in exactly one table**; one row deleted on purpose was found by its offset and alerted on; another was replayed by the tool and accounted for | [**integrity/README.md**](docs/integrity/README.md) |
| **AWS end to end**           | 5,000 events through ingestion, Kafka, Flink and Iceberg on S3/Glue: **Athena counted 5,000 rows, 5,000 distinct**                                                                                                              | [**aws.md**](docs/aws.md)                           |
| **Security on EKS**          | Kafka mTLS and ACLs 10/10, network policies 9/9, admission 15/15; signed image admitted, unsigned refused                                                                                                                       | [**SECURITY.md**](SECURITY.md)                      |
| **Observability**            | 31 alerts with runbooks and promtool unit tests; 57 of 57 dashboard queries return data; an alert fired, its runbook fixed it, it resolved                                                                                      | [**observability.md**](docs/observability.md)       |
| **Agentic ops (guardrails)** | 37 of 37 guardrail checks; 13 MCP tools reached through Amazon Bedrock AgentCore Gateway inside the VPC (1.38 s per call from the Runtime)                                                                                      | [**ai-ops.md**](docs/ai-ops.md)                     |
| **Teardown**                 | `aws-down` releases every volume and load balancer before `terraform destroy`, then checks for leftovers                                                                                                                        | [**aws.md**](docs/aws.md)                           |

### **How it works**

One infographic per flow ([**all diagrams**](docs/diagrams/README.md)):

| **Flow**                                                                                | **In one line**                                                                                       |
| --------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| [**Event to Iceberg, exactly once**](docs/diagrams/workflow-ingestion-exactly-once.png) | Client ids, `acks=all`, de-duplication in Flink state, Iceberg commit tied to the checkpoint          |
| [**Iceberg maintenance with Spark**](docs/diagrams/workflow-spark-maintenance.png)      | Scheduled compaction, manifest rewrite, snapshot expiry and aggregates while the stream keeps writing |
| [**GitOps delivery**](docs/diagrams/workflow-gitops-delivery.png)                       | Git, in-cluster Gitea, Argo CD app of apps, per-layer projects, admission, drift correction           |
| [**AWS environment lifecycle**](docs/diagrams/workflow-aws-lifecycle.png)               | Plan, human confirmation, apply, GitOps bootstrap, smoke and security tests, teardown                 |
| [**Software supply chain**](docs/diagrams/workflow-supply-chain.png)                    | Locked dependencies, Trivy gate, SBOM, Cosign signature with a KMS key, verification at admission     |
| [**Kafka mutual TLS**](docs/diagrams/workflow-kafka-mtls.png)                           | Strimzi CAs, one KafkaUser per client, certificates distributed by Kyverno, one mTLS listener         |
| [**Agentic ops loop**](docs/diagrams/workflow-agentic-ops.png)                          | Alert, investigation through MCP tools, typed diagnosis, Slack approval, single-use token, audit      |

![Event to Iceberg, exactly once](docs/diagrams/workflow-ingestion-exactly-once.png)

![Agentic ops loop](docs/diagrams/workflow-agentic-ops.png)

### **Skills map**

How the requirements of a senior staff data-platform role map to this repository.

| **Requirement**                                | **Where**                                                                                                                          | **Evidence**                                                                                                                 |
| ---------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| **Distributed systems design**                 | [**ADRs 0001-0012**](docs/architecture-decisions/), [**architecture**](docs/diagrams/README.md)                                    | Each decision with its alternatives and consequences; failure modes tested by chaos experiments                              |
| **Java, JVM internals, GC, profiling**         | [**`apps/ingestion-service`**](apps/ingestion-service/README.md), [**`apps/flink-iceberg-job`**](apps/flink-iceberg-job/README.md) | JFR profiles, G1 vs generational ZGC, an allocation hot spot found and removed ([**perf**](docs/performance/README.md))      |
| **Concurrency, networking, OS**                | Ingestion service: virtual threads, bounded in-flight work (429), JDK HTTP server; open-loop load generator                        | Latency measured from intended send time; [**ADR 0003**](docs/architecture-decisions/0003-ingestion-service-design.md)       |
| **Kafka at scale, HA**                         | [**`helm/charts/kafka-cluster`**](helm/charts/kafka-cluster): Strimzi, KRaft, 3 zones, RF=3, min ISR 2, rack awareness             | Broker isolation and zone outage without data loss ([**chaos**](docs/chaos/README.md))                                       |
| **Flink, stateful and exactly once**           | Flink job: keyed de-duplication state, checkpoints and savepoints on S3, Iceberg two-phase commit                                  | Pod kill and savepoint restore tests ([**flink-iceberg.md**](docs/flink-iceberg.md))                                         |
| **Spark, batch, partitioning**                 | [**`apps/spark-jobs`**](apps/spark-jobs/README.md): PySpark maintenance and hourly aggregates on the Spark operator                | Compaction under load, shuffle partitions and AQE experiment ([**spark-iceberg.md**](docs/spark-iceberg.md))                 |
| **Iceberg, schema evolution, encryption**      | REST catalog (local) and Glue (AWS); SSE-KMS on S3; [**schema evolution demo**](docs/flink-iceberg.md#schema-evolution-demo)       | Table-health exporter and alerts; maintenance with time-travel retention                                                     |
| **Parquet, Avro, compaction, governance**      | Avro on the wire with a registry (Apicurio), Parquet at rest, scheduled compaction and expiry                                      | Small-file and snapshot alerts with runbooks                                                                                 |
| **Kubernetes workloads, upgrades, monitoring** | kind (4 nodes, 3 zones) and EKS 1.37; PDBs, requests and limits, Pod Security `restricted`                                         | kube-prometheus-stack, Loki, Tempo, dashboards as code ([**observability.md**](docs/observability.md))                       |
| **Helm with overlays across environments**     | [**`helm/`**](helm/README.md): umbrella chart, library chart, values layered base, local, dev, staging, prod, aws                  | `make helm-check` renders and validates every environment                                                                    |
| **GitOps and release engineering**             | [**`gitops/`**](gitops/README.md): Argo CD app of apps, ApplicationSets, projects per layer, in-cluster Gitea                      | Same bootstrap on kind and EKS; signed images; hash-locked dependencies                                                      |
| **Relational database**                        | Postgres: Iceberg catalog, registry, agent audit store; RDS Postgres 17 on AWS (TLS only, KMS)                                     | Audit schema `ops.*` with every investigation, approval and execution                                                        |
| **Security methods and protocols**             | Kafka mTLS and ACLs, IRSA, KMS everywhere, Kyverno, default-deny network policies, Cosign, CloudTrail, Config                      | [**SECURITY.md**](SECURITY.md), [**NIST SP 800-53 mapping**](docs/security/control-mapping.md)                               |
| **Replication, fault tolerance, HA**           | Kafka replication across zones, Flink checkpoints and HA metadata on S3, two Alertmanagers across zones on AWS                     | [**chaos results**](docs/chaos/README.md) with the findings each experiment produced                                         |
| **Production troubleshooting**                 | A runbook for every alert ([**runbooks**](docs/runbooks/)); SLOs with multi-window burn-rate alerts                                | Chaos write-ups, "what went wrong" sections on every results page                                                            |
| **AI in engineering and operations**           | [**`apps/ai-ops`**](docs/ai-ops.md): Pydantic AI agent, MCP tools, Slack approvals, Langfuse, Hermes Agent, AgentCore on AWS       | Guardrail tests, recorded eval scenarios; the repo itself was built with Claude Code ([**below**](#author-and-contributors)) |

### **Phases**

| **Phase** | **Scope**                                                                                                      | **Status**                                 |
| --------- | -------------------------------------------------------------------------------------------------------------- | ------------------------------------------ |
| **0**     | Baseline cleanup, git, cost guardrails                                                                         | done                                       |
| **1**     | Local kind platform: Strimzi Kafka, Apicurio, SeaweedFS, Iceberg REST catalog, Postgres, kube-prometheus-stack | done                                       |
| **2**     | Java ingestion service (Avro, Micrometer, JFR baseline)                                                        | done                                       |
| **3**     | Flink to Iceberg job (exactly once, savepoint restart test, schema evolution)                                  | done                                       |
| **4**     | PySpark jobs (compaction, snapshot expiry, aggregates, tuning)                                                 | done                                       |
| **5**     | Helm umbrella chart, environment overlays, Argo CD                                                             | done                                       |
| **6**     | AWS via Terraform (VPC, EKS, IRSA, KMS, S3, Glue, Athena, RDS, ECR)                                            | done                                       |
| **7**     | Security and compliance (Kafka mTLS, Kyverno, network policies, SBOM, signing, audit, NIST 800-53 mapping)     | done                                       |
| **8**     | Observability (SLOs, burn-rate alerts, dashboards as code, Loki, Tempo, Iceberg exporter) and chaos tests      | done                                       |
| **9**     | Agentic ops: MCP tools, Pydantic AI agent, approvals, Langfuse, Hermes Agent, AgentCore, eval suite            | done; live model runs await Bedrock access |
| **10**    | Docs and portfolio: README, skills map, architecture diagrams with official icons, workflow infographics       | done                                       |

Next, outside the phases: run the agent and its eval suite once Bedrock access to Claude is granted; capture the three remaining eval scenarios; a web dashboard (Cognito and CloudFront) was planned as a thin product view and is not built.

---

## **Requirements**

| **Tool**                    | **Used** | **Needed for**                                                               |
| --------------------------- | -------- | ---------------------------------------------------------------------------- |
| **Terraform 1.10+**         | Yes      | The AWS environment (`terraform/`; AWS provider `~> 6.0`)                    |
| **AWS Console**             | Yes      | Viewing EKS, Athena and budgets; requesting Bedrock model access             |
| **AWS CLI v2**              | Yes      | Credentials, ECR login, leftover checks in `make aws-down`                   |
| **Python 3.13+ and uv**     | Yes      | Test tools, diagrams, dependency locks (the agent images run Python 3.14)    |
| **Git**                     | Yes      | Source control; Argo CD pulls from an in-cluster Gitea                       |
| **Docker Desktop (29.x)**   | Yes      | Everything local; give it at least 20 GB of memory (see below)               |
| **kind, kubectl, helm**     | Yes      | The local Kubernetes cluster and the charts                                  |
| **JDK 21**                  | Yes      | Building the Java services (Gradle provisions JDK 17 for the Flink job)      |
| **make, curl, jq, openssl** | Yes      | The scripts behind every `make` target                                       |
| **trivy**                   | Yes      | `make security-scan`                                                         |
| **Graphviz (`dot`)**        | Yes      | `make diagrams` only                                                         |
| **Jenkins**                 | No       | No CI server: the test gate is `make` targets, and Argo CD does the delivery |
| **GCP Console, Google SDK** | No       | The project runs on kind and AWS only                                        |

The local cluster uses about 13 GiB at rest and about 4 GiB more per Spark job; 20 GB for Docker leaves room for that ([**memory notes**](docs/lab/RUNBOOK.md#memory)).

On Windows, run every command in **Git Bash**. An AWS account is needed only for Part 9; it costs about $0.40 per hour while the environment is up.

`make local-up` installs the platform below through Argo CD; the versions are pinned in `local/scripts/lib.sh` and `gitops/`.

| **Component**                         | **Version**                | **Namespace**            | **Role**                                                                                      |
| ------------------------------------- | -------------------------- | ------------------------ | --------------------------------------------------------------------------------------------- |
| **kube-prometheus-stack**             | chart 92.14.1              | `monitoring`             | Prometheus, Alertmanager, Grafana                                                             |
| **Strimzi operator + Kafka**          | 1.3.0 / 4.3.1              | `kafka`                  | KRaft, 3 nodes across 3 zones, RF=3, min ISR=2                                                |
| **Postgres**                          | 17                         | `data`                   | Iceberg catalog, Apicurio, the agent's audit store, Langfuse                                  |
| **SeaweedFS**                         | 4.30                       | `data`                   | Local S3 stand-in ([**ADR 0002**](docs/architecture-decisions/0002-seaweedfs-as-local-s3.md)) |
| **Iceberg REST catalog**              | 1.10.1 (+ Postgres driver) | `data`                   | JDBC catalog in Postgres, data in S3                                                          |
| **Apicurio Registry**                 | 3.2.1                      | `data`                   | Avro schemas, Confluent-compatible API                                                        |
| **Flink Kubernetes Operator + Flink** | 1.16.1 / 2.1.3             | `flink-operator`, `apps` | The Kafka-to-Iceberg job                                                                      |
| **Spark operator + Spark**            | 2.5.2 / 4.1.4              | `spark-operator`, `apps` | PySpark maintenance and aggregate jobs (Iceberg 1.12.0)                                       |
| **Argo CD**                           | 3.5.6 (chart 10.11.6)      | `argocd`                 | Deploys and reconciles everything else from Git                                               |
| **Gitea**                             | 28.2.0                     | `gitops`                 | In-cluster Git server Argo CD pulls from                                                      |
| **Kyverno**                           | 1.19.2 (chart 3.9.2)       | `kyverno`                | Admission policies, Kafka certificate distribution                                            |
| **External Secrets Operator**         | 2.14.0                     | `external-secrets`       | Syncs credentials from AWS Secrets Manager (no store locally)                                 |
| **Loki, Tempo, Grafana Alloy**        | 3.6 / 2.9 / 1.21           | `observability`          | Logs and traces                                                                               |
| **Chaos Mesh**                        | 2.8.5                      | `chaos-mesh`             | Fault injection, limited to the platform namespaces                                           |
| **iceberg-exporter**                  | 0.1.0                      | `apps`                   | Iceberg table-health metrics                                                                  |
| **Ops agent**                         | 0.1.0                      | `ai-ops`                 | mcp-platform-tools, ops-agent, ops-control ([**ai-ops.md**](docs/ai-ops.md))                  |
| **Hermes Agent**                      | 0.19.0                     | `ai-ops`                 | Interactive on-call assistant, read-only                                                      |
| **Langfuse**                          | 4.56.0 (chart 2.1.5)       | `langfuse`               | LLM tracing for the agent, self-hosted                                                        |

---

## **Project Structure**

```text
full-cloud-dashboard-project/
├── apps/                         # Application code, one folder per service
│   ├── ai-ops/                   # Ops agent, approval service, eval runner
│   ├── flink-iceberg-job/        # Flink job (Java 17): Kafka to Iceberg, exactly once
│   ├── hermes/                   # Hermes Agent image (on-call assistant)
│   ├── iceberg-exporter/         # Prometheus exporter for Iceberg table health
│   ├── ingestion-service/        # Java 21 HTTP ingestion service and load generator
│   ├── mcp-platform-tools/       # MCP server: 13 read-only platform tools
│   └── spark-jobs/               # PySpark: Iceberg maintenance and hourly aggregates
├── chaos/experiments/            # Chaos Mesh faults: broker isolation, latency, pod kill
├── docs/                         # Documentation (index.md)
│   ├── architecture-decisions/   # ADRs 0001-0012
│   ├── chaos/                    # Chaos experiment results
│   ├── diagrams/                 # Architecture diagrams and workflow infographics
│   ├── lab/                      # Lab runbook (RUNBOOK.md) and screenshots (images/)
│   ├── performance/              # JVM performance results
│   ├── runbooks/                 # Alert runbooks, also read by the ops agent
│   ├── security/                 # NIST SP 800-53 control mapping
│   └── *.md                      # Results: flink-iceberg, spark-iceberg, aws, observability, ai-ops
├── evals/agent-scenarios/        # Recorded incidents for the agent eval suite
├── gitops/                       # Argo CD
│   ├── argocd/                   # Applications and ApplicationSets
│   ├── bootstrap/                # Argo CD, Gitea, projects, root app
│   └── operators/                # Operator values per environment (local, aws)
├── helm/                         # Helm
│   ├── charts/                   # Umbrella chart (platform), library chart, 13 sub-charts
│   └── values/                   # Overlays: local, aws, dev, staging, prod
├── local/                        # Local platform
│   ├── container-images/         # Iceberg REST catalog image with the Postgres driver
│   ├── scripts/                  # Scripts behind every make target
│   ├── tools/                    # Dashboard, diagram, table and test tools
│   └── kind-config.yaml          # 4-node kind cluster, 3 worker zones
├── terraform/                    # AWS infrastructure
│   ├── bootstrap/                # Account level: state bucket, budgets, audit trail
│   ├── envs/dev/                 # Ephemeral EKS environment
│   └── modules/cost-guardrails/  # Budget alerts module
├── .gitignore                    # Git ignore rules
├── .trivyignore                  # Trivy ignore rules
├── CODE_OF_CONDUCT.md            # Code of conduct for contributors
├── CONTRIBUTING.md               # Tests per component, change checklist
├── LICENSE.txt                   # MIT
├── Makefile                      # Every command (make help)
├── README.md                     # This file
└── SECURITY.md                   # Security controls and vulnerability reporting
```

Each app has its own README; the documentation index is [**docs/index.md**](docs/index.md).

---

## **Steps to complete the lab**

The full lab, with sign-ins, expected output and screenshot instructions, is [**docs/lab/RUNBOOK.md**](docs/lab/RUNBOOK.md). The steps below are its summary.

| **Part**                                      | **Time**              | **Cost**                              |
| --------------------------------------------- | --------------------- | ------------------------------------- |
| **1. Local platform up**                      | 20-40 min (first run) | free                                  |
| **2. Tour of the UIs**                        | 15 min                | free                                  |
| **3. Events, exactly once, schema evolution** | 20 min                | free                                  |
| **4. Spark maintenance and aggregates**       | 15 min                | free                                  |
| **5. GitOps change and drift**                | 10 min                | free                                  |
| **6. Security checks**                        | 15 min                | free                                  |
| **7. Alerts and a chaos experiment**          | 20 min                | free                                  |
| **8. The ops agent**                          | 20 min                | free (model runs need Bedrock access) |
| **9. AWS environment (optional)**             | 1.5-2 h               | about $0.40 per hour while up         |

1. **Bring up the local platform.** `make local-up` creates a 4-node kind cluster, builds and loads the images, installs Argo CD and Gitea, publishes the working tree, and waits until Argo CD has installed everything.

   ```bash
   make local-up        # 20-40 min the first time
   make gitops-status   # every application Synced and Healthy
   make local-smoke     # end-to-end checks, ending with "All smoke checks passed."
   make local-ports     # in a second terminal: Argo CD :8090, Gitea :3300, Grafana :3000, Prometheus :9090, Langfuse :3001 (when on)
   ```

2. **Tour the UIs.** Grafana (`admin` / `admin-local-dev`) has five Lakehouse dashboards; Prometheus shows every scrape job up (`min by (job) (up)`); Alertmanager shows only `Watchdog`; the Avro schema is at `http://localhost:8085/apis/ccompat/v7/subjects/events.raw-value/versions/latest`.

3. **Send events, prove exactly once, evolve the schema.** The exactly-once test and the schema demo are destructive: they reset the topic and the tables.

   ```bash
   make local-loadtest RATE=5000 DURATION=60
   make local-test-exactly-once        # 3 waves, a TaskManager kill, a savepoint restore, 1,000 re-sends
   make local-demo-schema-evolution    # adds an optional column from producer to table without downtime
   make local-test-integrity           # every Kafka offset accounted for; a deleted row is found, alerted on, replayed
   ```

4. **Run Spark maintenance and aggregates.** Send some load first so there are small files to compact.

   ```bash
   make local-deploy-spark
   make local-spark-maintenance   # fewer, larger files and fewer manifests, same row count
   make local-spark-aggregate
   make local-spark-schedules
   ```

5. **Make a GitOps change and watch drift being corrected.** Edit something under `helm/`, then:

   ```bash
   make helm-check && make gitops-push && make gitops-status
   kubectl --context kind-lakehouse -n apps scale deployment iceberg-exporter --replicas=0   # Argo CD puts it back
   ```

6. **Run the security checks.**

   ```bash
   make security-scan   # Trivy gate, SBOMs, Terraform and Helm misconfigurations
   make security-test   # Kafka mTLS and ACLs, network policies, admission policies
   ```

7. **Test the alerts and run a chaos experiment.**

   ```bash
   make rules-test
   make chaos EXP=broker-isolation   # one broker cut off for 6 minutes under load, then an exactly-once check
   ```

8. **Test the ops agent.** Everything except live model answers works without Bedrock access.

   ```bash
   make ai-test        # unit tests
   make ai-deploy      # MCP tools, agent, approval service, Hermes
   make langfuse-on    # optional: LLM tracing (about 2 GiB; off by default)
   make ai-ops-test    # 37 guardrail checks, including a real approved action and its audit trail
   ```

9. **Optional: the AWS environment.** Once per account, `terraform/bootstrap` creates the state bucket, budget alerts ($100/month), CloudTrail, AWS Config and IAM Access Analyzer:

   ```bash
   cd terraform/bootstrap
   cp terraform.tfvars.example terraform.tfvars   # set budget_alert_emails
   mv backend.tf backend.tf.off && terraform init && terraform apply
   mv backend.tf.off backend.tf
   terraform init -migrate-state -backend-config="bucket=lakehouse-tfstate-$(aws sts get-caller-identity --query Account --output text)"
   ```

   Then, per demo (read the plan before applying):

   ```bash
   make aws-init aws-plan
   make aws-apply
   make aws-up                              # images to ECR (signed), then the same GitOps bootstrap as kind
   make aws-smoke aws-security-test
   make aws-agentcore aws-ai-test           # optional: the agent on Amazon Bedrock AgentCore
   ```

![AWS dev environment](docs/diagrams/aws-deployment.png)

---

## **Artifacts and Screenshots**

Captured on the local kind cluster on 2026-11-13 (Parts 1-8) and on EKS on 2026-11-01 (Part 9). Usernames, host names and browser profiles are blurred.

| **#**  | **Screenshot**                                    | **Part** |
| ------ | ------------------------------------------------- | -------- |
| **01** | Argo CD: every application Synced and Healthy     | 1        |
| **02** | Gitea: the repository Argo CD deploys from        | 1        |
| **03** | `make local-smoke` passing                        | 1        |
| **04** | Grafana: Lakehouse / Overview                     | 2        |
| **05** | Prometheus: every scrape job up                   | 2        |
| **06** | Apicurio: the event schema                        | 2        |
| **07** | Grafana: Ingestion SLO during the load test       | 3        |
| **08** | Exactly-once test result                          | 3        |
| **09** | Grafana: Pipeline during the exactly-once tests   | 3        |
| **10** | Grafana: Tables before and after compaction       | 4        |
| **11** | Spark maintenance summary                         | 4        |
| **12** | Argo CD: the sync of a change                     | 5        |
| **13** | `make security-test` summary                      | 6        |
| **14** | Alertmanager: alerts firing during the experiment | 7        |
| **15** | Grafana: Kafka during the broker outage           | 7        |
| **16** | Chaos experiment result                           | 7        |
| **17** | `make ai-ops-test` passing                        | 8        |
| **19** | AWS console: the EKS cluster and its nodes        | 9        |

Screenshot 18 (a Langfuse trace) needs Bedrock model access and was skipped; 20-22 (Athena, Argo CD on EKS, the teardown check) were not captured.

### **01. Argo CD: every application Synced and Healthy**

![Argo CD applications, part 1](docs/lab/images/01-argocd-applications-pt1.png)
![Argo CD applications, part 2](docs/lab/images/01-argocd-applications-pt2.png)

### **02. Gitea: the repository Argo CD deploys from**

![Gitea repository](docs/lab/images/02-gitea-repository.png)

### **03. `make local-smoke` passing**

![Local smoke test](docs/lab/images/03-local-smoke.png)

### **04. Grafana: Lakehouse / Overview**

![Grafana overview](docs/lab/images/04-grafana-overview.png)

### **05. Prometheus: every scrape job up (31 jobs)**

![Prometheus targets](docs/lab/images/05-prometheus-targets.png)

### **06. Apicurio: the event schema**

![Apicurio schema](docs/lab/images/06-apicurio-schema.png)

### **07. Grafana: Ingestion SLO during the load test**

![Grafana ingestion SLO](docs/lab/images/07-grafana-ingestion-slo.png)

### **08. Exactly-once test: 24,000 rows, 24,000 distinct, 0 duplicates**

![Exactly-once result](docs/lab/images/08-exactly-once-result.png)

### **09. Grafana: Pipeline during the exactly-once tests**

![Grafana pipeline](docs/lab/images/09-grafana-pipeline.png)

### **10. Grafana: Tables before and after compaction (small files 150 to 0)**

![Grafana tables compaction](docs/lab/images/10-grafana-tables-compaction.png)

### **11. Spark maintenance: 156 data files to 5, 77 manifests to 1, 366,350 rows before and after**

![Spark maintenance summary](docs/lab/images/11-spark-maintenance-summary.png)

### **12. Argo CD: the sync of a change**

![Argo CD sync](docs/lab/images/12-argocd-sync.png)

### **13. `make security-test`: Kafka, network policy and admission checks**

![Security test, part 1](docs/lab/images/13-security-test-pt1.png)
![Security test, part 2](docs/lab/images/13-security-test-pt2.png)

### **14. Alertmanager: `KafkaBrokerDown` firing during broker isolation**

![Alertmanager firing](docs/lab/images/14-alertmanager-firing.png)

### **15. Grafana: Kafka during the broker outage**

![Grafana Kafka outage](docs/lab/images/15-grafana-kafka-outage.png)

### **16. Chaos experiment: 63,850 events sent, 63,850 in Iceberg, 0 missing, 0 duplicates**

![Chaos result](docs/lab/images/16-chaos-result.png)

### **17. `make ai-ops-test`: 37 guardrail checks passing**

![Ops agent guardrail test, part 1](docs/lab/images/17-ai-ops-test-pt1.png)
![Ops agent guardrail test, part 2](docs/lab/images/17-ai-ops-test-pt2.png)

### **18. Langfuse trace**

![Langfuse trace](docs/lab/images/18-langfuse-trace.png)

### **19. AWS console: the EKS cluster with three Spot nodes in three zones**

![AWS EKS cluster](docs/lab/images/19-aws-eks-cluster.png)

### **20. Athena on AWS: 5,000 rows, 5,000 distinct**

![Athena count on AWS](docs/lab/images/20-aws-athena-count.png)

### **21. Argo CD on EKS: all 14 applications Synced and Healthy**

![Argo CD on EKS, part 1](docs/lab/images/21-argocd-eks-pt1.png)
![Argo CD on EKS, part 2](docs/lab/images/21-argocd-eks-pt2.png)

### **22. `make aws-down`: teardown with no leftovers**

![aws-down leftover check](docs/lab/images/22-aws-down-clean.png)

---

## **Teardown**

| **You want to**                     | **Run**                                                                                    |
| ----------------------------------- | ------------------------------------------------------------------------------------------ |
| **Remove the AWS environment**      | `make aws-down` (the bootstrap stack stays by design)                                      |
| **Pause the lab and keep its data** | `docker stop lakehouse-control-plane lakehouse-worker lakehouse-worker2 lakehouse-worker3` |
| **Resume later**                    | `docker start ...` the same four, then `make gitops-status` and `make local-smoke`         |
| **Delete the local cluster**        | `make local-down` (rebuilding takes 20-40 minutes)                                         |
| **Stop Docker Desktop**             | `docker desktop stop`                                                                      |

`make aws-down` runs in an order that leaves nothing behind:

1. It deletes the Kubernetes volumes and load balancers first, so AWS releases the EBS volumes and NLBs.
2. It runs `terraform destroy`.
3. It deletes the environment's CloudWatch log groups and the older local images.
4. It checks for leftovers: EBS volumes, load balancers, VPC, EKS, RDS and the Terraform state.

If the agent's AgentCore Runtime was created, AWS can take up to 8 hours to release its network interfaces, and the VPC cannot be deleted until then. Wait, then run `make aws-down` again. Budget alerts notify; they do not stop spend, so always destroy the environment when you are done.

---

## **Lessons Learned**

### **What Is Relatable to a Customer**

- **Exactly once is a design, not a setting.** Client-assigned ids, `acks=all`, de-duplication state and an Iceberg commit tied to the Flink checkpoint together survive retries, pod kills and restarts; any one alone does not.
- **Streaming creates small files.** Flink commits every 10 seconds, and the lab's table reached 156 small files; compaction cut them to 5 and the full-scan time from 25.8 s to 4.7 s on the same 366,350 rows.
- **Ephemeral environments keep cloud cost predictable.** The whole EKS environment is created, tested and destroyed from code, so it costs only while someone is using it.
- **AI in operations needs guardrails.** The agent reads through 13 read-only tools and can only propose; a person approves, and a single-use token limits it to that exact action.

### **Struggles**

- **Storage pressure stalled Flink.** A 3-second checkpoint interval with full-state snapshots filled the local S3 stand-in; Flink stalled with 500,000 events waiting, then drained them exactly once when storage was fixed ([**spark-iceberg.md**](docs/spark-iceberg.md#what-went-wrong-and-what-it-taught)).
- **Recovery without HA duplicated data.** A failed job without Flink HA restarted from an old savepoint and wrote 500,000 duplicate rows; HA is now required by the chart ([**ADR 0004**](docs/architecture-decisions/0004-flink-to-iceberg-pipeline.md)).
- **The model was not available.** The AWS account has no Bedrock access to Claude, so everything around the agent was verified without a single live diagnosis.
- **Windows got in the way.** Windows reserved port ranges, the Docker VM clock drifted (Kafka re-elected its controller about 190 times an hour), and `jq.exe` line endings broke scripts.
- **Upstream bugs.** A Flink metrics bug (FLINK-35321) logged a warning on every checkpoint and is not fixed in any release yet; the job's log configuration filters that one message.
- **Each AWS run found something new.** IAM for the AgentCore Gateway, a credential prefix with two spaces, an MCP server that crashed only with TLS on ([**aws.md**](docs/aws.md), [**ai-ops.md**](docs/ai-ops.md)).

### **Saving Money After Teardown**

- **About $0.40 per hour while up**, on three Spot nodes, and nothing while down: the dev Terraform state is empty after `make aws-down`.
- **What stays:** the bootstrap stack (state bucket, KMS key, CloudTrail, AWS Config, budget alerts) is small and kept on purpose; it holds the Terraform state and the audit trail.
- **Challenges:** AgentCore's network interfaces held the VPC for 8 h 15 min after the Runtime was deleted; EKS recreated its log group after `terraform destroy`, which broke the next apply; Loki's and Tempo's 20 GiB volumes were left once because `aws-down` did not yet delete their namespace. Each is now handled by `aws-down` ([**aws.md**](docs/aws.md)).

---

## **References**

### **Documentation**

| **Topic**                     | **Link**                                                                                           |
| ----------------------------- | -------------------------------------------------------------------------------------------------- |
| **Terraform AWS provider**    | **<https://registry.terraform.io/providers/hashicorp/aws/latest/docs>**                            |
| **Amazon EKS**                | **<https://docs.aws.amazon.com/eks/latest/userguide/what-is-eks.html>**                            |
| **Amazon Bedrock AgentCore**  | **<https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/what-is-bedrock-agentcore.html>** |
| **Amazon Athena and Iceberg** | **<https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html>**                           |
| **Apache Kafka**              | **<https://kafka.apache.org/documentation/>**                                                      |
| **Strimzi**                   | **<https://strimzi.io/documentation/>**                                                            |
| **Apache Flink**              | **<https://nightlies.apache.org/flink/flink-docs-release-2.1/>**                                   |
| **Flink Kubernetes Operator** | **<https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/>**                    |
| **Apache Iceberg**            | **<https://iceberg.apache.org/docs/latest/>**                                                      |
| **Apache Spark**              | **<https://spark.apache.org/docs/latest/>**                                                        |
| **Argo CD**                   | **<https://argo-cd.readthedocs.io/>**                                                              |
| **Gitea**                     | **<https://gitea.com/>**                                                                           |
| **Helm**                      | **<https://helm.sh/docs/>**                                                                        |
| **kind**                      | **<https://kind.sigs.k8s.io/>**                                                                    |
| **Kyverno**                   | **<https://kyverno.io/docs/>**                                                                     |
| **Chaos Mesh**                | **<https://chaos-mesh.org/docs/>**                                                                 |
| **Model Context Protocol**    | **<https://modelcontextprotocol.io/>**                                                             |
| **Pydantic AI**               | **<https://ai.pydantic.dev/>**                                                                     |
| **Langfuse**                  | **<https://langfuse.com/docs>**                                                                    |
| **Trivy**                     | **<https://trivy.dev/>**                                                                           |
| **NIST SP 800-53 Rev. 5**     | **<https://csrc.nist.gov/pubs/sp/800/53/r5/upd1/final>**                                           |
| **FLINK-35321**               | **<https://issues.apache.org/jira/browse/FLINK-35321>**                                            |

### **GitHub repositories**

| **Project**                   | **Repository**                                            |
| ----------------------------- | --------------------------------------------------------- |
| **Apache Flink**              | **<https://github.com/apache/flink>**                     |
| **Flink Kubernetes Operator** | **<https://github.com/apache/flink-kubernetes-operator>** |
| **Apache Iceberg**            | **<https://github.com/apache/iceberg>**                   |
| **Apache Spark**              | **<https://github.com/apache/spark>**                     |
| **Strimzi**                   | **<https://github.com/strimzi/strimzi-kafka-operator>**   |
| **Apache Kafka**              | **<https://github.com/apache/kafka>**                     |
| **Kubeflow Spark operator**   | **<https://github.com/kubeflow/spark-operator>**          |
| **Argo CD**                   | **<https://github.com/argoproj/argo-cd>**                 |
| **Kyverno**                   | **<https://github.com/kyverno/kyverno>**                  |
| **Chaos Mesh**                | **<https://github.com/chaos-mesh/chaos-mesh>**            |
| **SeaweedFS**                 | **<https://github.com/seaweedfs/seaweedfs>**              |
| **Apicurio Registry**         | **<https://github.com/Apicurio/apicurio-registry>**       |
| **Pydantic AI**               | **<https://github.com/pydantic/pydantic-ai>**             |
| **Langfuse**                  | **<https://github.com/langfuse/langfuse>**                |
| **Hermes Agent**              | **<https://github.com/NousResearch/hermes-agent>**        |

---

## **Troubleshooting**

| **Command**                                                                                 | **What it showed**                                                     |
| ------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| `kubectl --context kind-lakehouse get pods -A`                                              | Pods that are not Running or keep restarting                           |
| `kubectl --context kind-lakehouse -n <ns> logs <pod> --previous`                            | Why a container crashed before its last restart                        |
| `kubectl --context kind-lakehouse -n apps get flinkdeployment events-to-iceberg`            | The Flink job's lifecycle and job state                                |
| `make gitops-status`                                                                        | Argo CD sync and health per application                                |
| `curl -s localhost:9090/api/v1/query --data-urlencode 'query=min by (job) (up)'`            | Which scrape jobs are down                                             |
| `docker restart lakehouse-control-plane`                                                    | Fixes `kubectl` hanging after Docker or WSL restarted                  |
| `netsh interface ipv4 show excludedportrange protocol=tcp`                                  | Ports Windows has reserved (`forbidden by its access permissions`)     |
| `wsl --shutdown`                                                                            | Resets the Docker VM clock when Kafka keeps re-electing its controller |
| `trivy image lakehouse/<app>:dev`                                                           | Which package a failed security gate is about                          |
| `kubectl --context kind-lakehouse -n ai-ops exec deploy/hermes -- hermes mcp test platform` | Whether Hermes reaches the MCP tool server                             |

More symptoms and fixes are in the runbook's [**Troubleshooting**](docs/lab/RUNBOOK.md#troubleshooting) table, and every alert has its own [**runbook**](docs/runbooks/).

### **What crashed**

- **SeaweedFS** was OOM-killed at its 1 GiB limit under combined load; the limit is now 3 GiB.
- **The ingestion service** stayed "Running" but never ready when Kafka's DNS name did not resolve at start-up; it now exits so Kubernetes restarts it.
- **The MCP tool server** crashed on EKS with TLS on: two listeners ran the app's start-up twice. Kind never ran the TLS listener, so only AWS showed it.
- **The Prometheus operator and Langfuse** restarted several times after the cluster came back from a stop, then ran normally without intervention.

### **Misconfigurations and version issues**

- **Flink 2.x supports Java 21 only experimentally**, so the job runs on `flink:2.1.3-java17` and its build uses a Java 17 toolchain.
- **Spark 4.1.4 with Iceberg 1.12.0 could not plan a MERGE from a DataFrame view**; the aggregation now sits inside the MERGE as a SQL subquery.
- **The catalog and the namespace are both called `lakehouse`**, so `lakehouse.events` was read as catalog plus table; jobs now pass three-part names.
- **Prometheus 3 shows only one scrape pool** on the Target health page when there are more than 20, so the runbook checks targets with a PromQL query instead.
- **Strimzi restarts every broker** when a version label changes, so Strimzi resources carry no version labels.
- **The AgentCore Gateway adds its own space** after the credential prefix, so `"Bearer "` became two spaces and every call was rejected.

### **systemctl and sudo**

Neither was needed. Every service runs as a Kubernetes workload, so the equivalents are `kubectl rollout restart` and Argo CD sync instead of `systemctl restart`. The host is Windows with Git Bash, and containers run as non-root under Pod Security `restricted`. The only step that needs elevated rights is resetting Windows' reserved ports (`net stop winnat` and `net start winnat` in an administrator shell).

---

## **Author and Contributors**

| **Role**         | **Name**                                     |
| ---------------- | -------------------------------------------- |
| **Author**       | T.I.Q.S. (solo project)                      |
| **Group**        | jerMutants - The Brotherhood of the Wolfpack |
| **Group leader** | John Sweeney                                 |

The project was built with Claude Code as a pair programmer: a written plan per phase, every change reviewed, and every claim in the docs backed by a command that was run. Results pages record single runs as single runs, list what each test does not prove, and keep a "what went wrong" section.

| **Version** | **Date**   | **Changes**                                                                    |
| ----------- | ---------- | ------------------------------------------------------------------------------ |
| **1.0**     | 2026-11-01 | All ten phases built; AWS environment applied, verified and destroyed          |
| **1.1**     | 2026-11-13 | Lab Parts 1-8 re-run with screenshots; README restructured to the lab template |

---

## **License**

[**MIT**](LICENSE.txt). Logos in the diagrams belong to their owners; see [**docs/diagrams/README.md**](docs/diagrams/README.md#logos-and-trademarks).
