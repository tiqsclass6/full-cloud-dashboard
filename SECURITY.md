# **Security Policy**

The platform's security controls, layer by layer, how each is tested, and the known gaps. Design decisions are in [**ADR 0008**](docs/architecture-decisions/0008-security-baseline.md); the mapping to NIST SP 800-53 is in [**docs/security/control-mapping.md**](docs/security/control-mapping.md).

## **Reporting vulnerabilities**

Please report a vulnerability privately through GitHub's private vulnerability reporting (the repository's **Security** tab, "Report a vulnerability"), not in a public issue. Include what is affected, how to reproduce it, and the impact you expect. This is a personal portfolio project, so replies are best effort.

## **Layers**

| **Layer**              | **Control**                                                                                                                                                                                               | **Where**                                                                 |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| **Kafka**              | TLS listener with client certificates (mutual TLS) only; one KafkaUser per client with least-privilege ACLs; listener reachable from `apps` only                                                          | `helm/charts/kafka-cluster`                                               |
| **Certificates**       | Strimzi issues and renews them; a Kyverno generating policy copies the client secrets to `apps` and keeps them in sync                                                                                    | `helm/charts/policies`                                                    |
| **Pods**               | Pod Security `restricted` enforced in `apps`, `data`, `kafka` (local `data`: `baseline`, see below); non-root, no capabilities, read-only root where possible                                             | `helm/charts/platform/templates/namespaces.yaml`, chart templates         |
| **Admission**          | Kyverno: requests and limits, pinned image versions, approved registries, ClusterIP-only Services (Deny in platform namespaces, Audit elsewhere); ops-control's Spark runs limited to the platform's jobs | `helm/charts/policies`                                                    |
| **Network**            | Default-deny NetworkPolicies, explicit cross-namespace flows, no pod access to instance metadata                                                                                                          | `helm/charts/network-policies`                                            |
| **Delivery**           | Argo CD projects per layer (operators, platform, root); the default project is empty                                                                                                                      | `gitops/argocd/project.yaml`, `gitops/bootstrap/projects.yaml`            |
| **Secrets (AWS)**      | External Secrets Operator syncs RDS and Grafana credentials from Secrets Manager; nothing secret in Git, Terraform state or scripts                                                                       | `helm/charts/secrets`, `terraform/envs/dev/secrets.tf`                    |
| **Data at rest (AWS)** | Customer-managed KMS keys for S3, EBS, RDS, EKS Secrets, ECR, Secrets Manager, Terraform state and audit logs                                                                                             | Terraform                                                                 |
| **In transit (AWS)**   | RDS refuses non-TLS (TLS 1.2+); S3 buckets deny non-TLS requests                                                                                                                                          | `terraform/envs/dev/database.tf`, `data.tf`                               |
| **Supply chain**       | Trivy gate and CycloneDX SBOMs; images signed by digest with a KMS key (Cosign); Kyverno refuses unsigned platform images on EKS                                                                          | `local/scripts/security-scan.sh`, `aws-images.sh`, `helm/charts/policies` |
| **Audit (AWS)**        | CloudTrail (all regions, validated, encrypted, one year); IAM Access Analyzer; GuardDuty HIGH findings emailed; AWS Config rules while an environment exists                                              | `terraform/bootstrap/audit.tf`, `terraform/envs/dev/config.tf`            |

## **Tests**

| **Command**                  | **Checks**                                                                                                                                                                                                                       |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`make security-scan`**     | Image vulnerabilities (gated), SBOMs, Terraform and Helm misconfigurations                                                                                                                                                       |
| **`make security-test`**     | Kafka: each identity allowed and refused per its ACLs, no unauthenticated or plaintext access. Network: allowed and blocked paths. Admission: each rule rejects its violation; Argo CD projects refuse out-of-scope applications |
| **`make aws-security-test`** | Signed image admitted, unsigned image refused; External Secrets synced; RDS TLS enforced; CloudTrail, Config and Access Analyzer working; the three cluster suites on EKS                                                        |

## **Known gaps and exceptions**

- **In-cluster HTTP is not encrypted.** The ingestion service, schema registry and Iceberg REST catalog speak plain HTTP inside the cluster. Network policies limit who can connect; encryption needs a service mesh or per-service TLS.
- **The EKS API endpoint is public**, limited to the operator's /32. Production would be private-only.
- **Local `data` namespace enforces `baseline`**, because the local stand-ins for managed services (Postgres, SeaweedFS) start as root. They do not exist on AWS.
- **Kyverno can read and write Secrets cluster-wide**, which the certificate distribution needs. Kyverno is treated as part of the control plane.
- **Vulnerabilities in upstream base distributions** (the Spark image's Ubuntu packages and bundled jars) are reported, not gated. Shaded libraries inside third-party jars are accepted with an expiry date (`.trivyignore.yaml`).
- **Image signatures are verified on AWS only**; locally images are loaded directly into kind.

## **Verified (2026-10-30)**

Single runs, on the local kind cluster and on one ephemeral EKS environment.

| **Check**                                    | **Local (kind)**                               | **AWS (EKS)**                                         |
| -------------------------------------------- | ---------------------------------------------- | ----------------------------------------------------- |
| **Platform smoke test**                      | 17/17                                          | 15/15; Athena counted 5,000 rows, 5,000 distinct      |
| **Kafka security (`kafka-security-test`)**   | 10/10                                          | 10/10                                                 |
| **Network policies (`network-policy-test`)** | 16/16 (all 7 "blocked" paths were open before) | 9/9 (local-only stand-ins skipped)                    |
| **Admission (`policy-test`)**                | 12/12                                          | 12/12                                                 |
| **Image signatures**                         | not applicable (no registry)                   | signed image admitted; unsigned image refused         |
| **External Secrets**                         | not applicable                                 | RDS and Grafana credentials synced                    |
| **RDS in transit**                           | not applicable                                 | `rds.force_ssl = 1`; registry connected over TLS      |
| **Audit services**                           | not applicable                                 | CloudTrail logging, Config recording, Analyzer active |
| **Spark job under all controls**             | maintenance run completed                      | hourly aggregate completed (Glue, IRSA)               |
| **`security-scan` gate**                     | passed (3 images, Terraform, Helm)             | passed before every push                              |

AWS Config results (dev): 7 of 9 rules compliant for the platform's resources.

- `eks-endpoint-no-public-access` is NON_COMPLIANT by design (public endpoint, restricted to one /32).
- `s3-bucket-ssl-requests-only` is NON_COMPLIANT only because of 8 pre-existing buckets in the account that this project does not manage; all 5 of the platform's buckets are compliant.

### **What the rollout found**

- **Kyverno 1.19 deprecates `ClusterPolicy`.** The first policy was written in the old form; it was rewritten as a `GeneratingPolicy`.
- **Kyverno's controllers need explicit read access to Secrets** for a policy that copies them (admission, background and reports controllers). The policy status reported the missing permission; it was granted and the controllers restarted.
- **A failing sync blocks newer commits.** Argo CD kept retrying the revision with the deprecated policy; the operation had to be terminated before it picked up the fix. The same happened on EKS when the `lakehouse` project did not yet allow `ImageValidatingPolicy`, a kind that renders only on AWS and so was never exercised locally.
- **The Flink image in kind was stale.** After the code change, the running job still used `PLAINTEXT` until the image was rebuilt and reloaded.
- **Spark driver pods were rejected by Pod Security.** The Spark operator applies `securityContext` fields only through its mutating webhook, which is disabled; the settings moved to Spark pod templates, which `spark-submit` applies itself.
- **Strimzi's helper pods had no resource requests or limits.** The audit-first rollout of the Kyverno rules reported them before enforcement could break anything.
- **Cosign 3 changed its defaults.** `--tlog-upload=false` now also needs `--use-signing-config=false`; the signing step failed until both were set.

### **Not verified**

- Certificate renewal end to end (the copy and sync were tested by deleting and tampering with a copied secret, not by a real CA renewal).
- RDS password rotation reaching Apicurio (the Secret follows within an hour; the pods need a restart).
- GuardDuty alert delivery (no HIGH finding occurred; sample findings were not generated, to avoid emailing test noise).
- The cost of Security Hub's Config-based checks during the run (billing data lags by about a day).
