# **Control mapping: NIST SP 800-53 Rev. 5**

How the platform's controls map to NIST SP 800-53 Rev. 5, the catalogue that FedRAMP baselines draw from. It covers what this repository implements and tests; it is not an assessment or an authorization package. Organisational controls (policy, training, personnel, physical) are out of scope.

Status: **Implemented** (in code, with an automated test), **Partial** (implemented with a known gap), **Planned** (a later phase).

## **Access control and identification**

| **Control**                       | **Status**  | **How**                                                                                                                                                       | **Evidence**                                              |
| --------------------------------- | ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------- |
| **AC-3 Access Enforcement**       | Implemented | Kafka ACLs per client (KafkaUser); Argo CD projects limit kinds and namespaces; IRSA policies per service account                                             | `kafka-security-test.sh`, `policy-test.sh` (Argo CD part) |
| **AC-4 Information Flow**         | Implemented | Default-deny NetworkPolicies with explicit flows; Kafka listener peers; S3 bucket policies deny non-TLS                                                       | `network-policy-test.sh`                                  |
| **AC-6 Least Privilege**          | Implemented | IRSA roles scoped to named buckets, one Glue database, one KMS key and named secrets; Kafka ACLs; Pod Security `restricted`; no Roles in the platform project | `kafka-security-test.sh`, `policy-test.sh`, Terraform     |
| **AC-17 Remote Access**           | Partial     | Kubernetes API reachable only from the operator's /32; no NodePort or LoadBalancer Services. Gap: the endpoint is public (private-only in production)         | `policy-test.sh`, Config `eks-endpoint-no-public-access`  |
| **IA-3 Device Identification**    | Implemented | Each Kafka client authenticates with its own certificate (mutual TLS); pods authenticate to AWS with web identity (IRSA)                                      | `kafka-security-test.sh`                                  |
| **IA-5 Authenticator Management** | Implemented | Strimzi issues and renews client certificates, Kyverno syncs them; RDS rotates its own password; Grafana's is written once, never stored in state or Git      | `aws-security-test.sh` (External Secrets)                 |

## **Audit and monitoring**

| **Control**                       | **Status**  | **How**                                                                                                                                       | **Evidence**                            |
| --------------------------------- | ----------- | --------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------- |
| **AU-2, AU-12 Event Logging**     | Implemented | CloudTrail (all regions, management events); EKS audit and authenticator logs; RDS connection logging; Kyverno policy reports                 | `aws-security-test.sh` (CloudTrail)     |
| **AU-9 Protection of Audit Info** | Implemented | KMS-encrypted, versioned audit bucket with `prevent_destroy` and TLS-only policy; CloudTrail log file validation (digest files)               | Terraform `bootstrap/audit.tf`          |
| **AU-11 Audit Record Retention**  | Implemented | 365 days (`audit_log_retention_days`)                                                                                                         | Terraform                               |
| **CA-7 Continuous Monitoring**    | Partial     | AWS Config rules while an environment exists; GuardDuty; Security Hub; Kyverno background scans; Trivy gate. Gap: Config is off between demos | `aws-security-test.sh` (Config results) |
| **SI-4 System Monitoring**        | Partial     | GuardDuty (EKS audit logs, S3, RDS logins) with HIGH findings emailed; Prometheus metrics. Gap: platform alert rules and runbooks are Phase 8 | Terraform `bootstrap/audit.tf`          |
| **IR-6 Incident Reporting**       | Partial     | GuardDuty HIGH findings go to an encrypted SNS topic and email. Gap: the ops agent triages platform alerts, not GuardDuty findings            | Terraform `bootstrap/audit.tf`          |

## **Configuration management**

| **Control**                     | **Status**  | **How**                                                                                                                             | **Evidence**                               |
| ------------------------------- | ----------- | ----------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------ |
| **CM-2 Baseline Configuration** | Implemented | Everything in the cluster is declared in Git and applied by Argo CD; AWS is declared in Terraform                                   | `make helm-check`, `terraform validate`    |
| **CM-3 Change Control**         | Implemented | Changes reach clusters only through Git; Argo CD reverts manual drift (self-heal); every `terraform apply` follows a reviewed plan  | gitops/README.md (drift test)              |
| **CM-6 Configuration Settings** | Implemented | Pod Security `restricted`; Kyverno rules; Trivy IaC scan of Terraform and Helm                                                      | `policy-test.sh`, `security-scan.sh`       |
| **CM-7 Least Functionality**    | Implemented | Read-only root filesystems, all capabilities dropped, no privilege escalation, no plaintext Kafka listener, ClusterIP-only Services | `policy-test.sh`, `kafka-security-test.sh` |
| **CM-8 Component Inventory**    | Implemented | CycloneDX SBOM per image (`build/security/*.cdx.json`)                                                                              | `security-scan.sh`                         |

## **System and communications protection**

| **Control**                             | **Status**  | **How**                                                                                                                                                         | **Evidence**                                     |
| --------------------------------------- | ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------ |
| **SC-7 Boundary Protection**            | Implemented | Private subnets; RDS reachable from the node security group only; network policies; instance metadata blocked for pods                                          | `network-policy-test.sh`                         |
| **SC-8 Transmission Confidentiality**   | Partial     | Kafka mutual TLS; RDS refuses non-TLS (TLS 1.2+); S3 TLS-only; AWS APIs over HTTPS. Gap: in-cluster HTTP (ingestion, schema registry, catalog) is not encrypted | `kafka-security-test.sh`, `aws-security-test.sh` |
| **SC-12, SC-13 Key Management, Crypto** | Implemented | Customer-managed KMS keys with rotation; the image-signing key is asymmetric and non-exportable                                                                 | Config `cmk-backing-key-rotation-enabled`        |
| **SC-28 Protection at Rest**            | Implemented | KMS for S3 (Iceberg data, checkpoints, results, state, audit), EBS, RDS, EKS Secrets, ECR and Secrets Manager                                                   | Config rules (S3, RDS, EBS, EKS)                 |

## **System integrity and supply chain**

| **Control**                                | **Status**  | **How**                                                                                                       | **Evidence**                             |
| ------------------------------------------ | ----------- | ------------------------------------------------------------------------------------------------------------- | ---------------------------------------- |
| **RA-5 Vulnerability Monitoring**          | Implemented | Trivy gate on every push; ECR scan on push; accepted risks recorded with an expiry date (`.trivyignore.yaml`) | `security-scan.sh`                       |
| **SI-2 Flaw Remediation**                  | Implemented | Dependency floors in the Gradle builds; the gate blocks pushes with fixable findings in our layers            | `security-scan.sh`                       |
| **SI-7 Software Integrity**                | Implemented | Images signed by digest with a KMS key; Kyverno refuses unsigned images (AWS); CloudTrail log validation      | `aws-security-test.sh` (signatures)      |
| **SR-3, SR-11 Supply Chain, Authenticity** | Implemented | Pinned versions everywhere; approved registries enforced at admission; SBOMs; signatures                      | `policy-test.sh`, `aws-security-test.sh` |

## **The ops agent (AI in operations)**

The agent of [**docs/ai-ops.md**](../ai-ops.md): it diagnoses alerts with a language model and proposes fixes that a person approves.

| **Control**                                                   | **Status**  | **How**                                                                                                                                                                     | **Evidence**                                              |
| ------------------------------------------------------------- | ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------- |
| **AC-5 Separation of Duties**                                 | Implemented | The component that reasons (ops-agent) cannot act; the component that acts (ops-control) runs only what a person approved; optional two-person approval                     | `ai-ops-test.sh`, `test_approvals.py`                     |
| **AC-6 Least Privilege**                                      | Implemented | Read-only tool identity without Secrets access; the agent has no Kubernetes token; the executor may run three named actions only; RBAC in its own chart and Argo CD project | `ai-ops-test.sh` (RBAC checks)                            |
| **AU-2, AU-12 Event Logging**                                 | Implemented | Every investigation, tool call, proposal, approval, denial and execution in Postgres (`ops.*`); tool calls in Loki; model and tool spans in Langfuse                        | `ai-ops-test.sh` (audit sequence)                         |
| **CM-3, CM-5 Change Control, Access Restrictions for Change** | Implemented | No change without an approval bound to the exact action and parameters (hash), signed single-use execution token, expiry                                                    | `test_approvals.py`, `ai-ops-test.sh`                     |
| **SI-10 Information Input Validation**                        | Implemented | The model's output is a validated schema; proposals validated twice against a closed catalogue; cluster text treated as data (prompt injection)                             | `test_agent.py`, `test_parts.py`, eval `prompt-injection` |
| **SC-28, SI-12 Protection and Handling of Information**       | Implemented | Credentials redacted before tool results reach the model, the audit store or Langfuse; Langfuse self-hosted so prompts stay in the cluster                                  | `test_tools.py` (redaction), `test_evals.py` (mask)       |
| **SA-11 Developer Testing**                                   | Partial     | Deterministic eval suite over recorded incidents (pydantic-evals). Gap: measured results need model access to the Claude models, which this account does not have yet       | `make ai-eval`                                            |
