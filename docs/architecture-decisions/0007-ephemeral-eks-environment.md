# **ADR 0007: Ephemeral EKS environment with Terraform, the Glue catalog and Athena**

- Status: accepted
- Date: 2026-10-29

## **Context**
Phases 1-5 run the platform on kind, deployed by Argo CD from an in-cluster Git server. Phase 6 runs the same platform on AWS. The environment is for demos: it must be cheap, created and destroyed in one session, and leave nothing billable behind. The repository has no remote, and the AWS account id should stay out of it.

## **Decisions**
1. **Two Terraform roots.**
   - `terraform/bootstrap` holds the S3 state bucket and the budget alerts. It is applied once and never destroyed (`prevent_destroy` on the bucket).
   - `terraform/envs/<env>` holds everything else for one environment, with its own state key (`lakehouse/envs/<env>/terraform.tfstate`) and S3-native locking (`use_lockfile`). Destroying an environment cannot touch the guardrails.
   - The state bucket name contains the account id, so it is passed at `init` time (`-backend-config`), not written in the code.
2. **Community modules for VPC and EKS, pinned exactly** (`terraform-aws-modules/vpc` 6.7.3, `eks` 21.29.0). Everything specific to the lakehouse (S3, KMS, IAM, Glue, Athena, RDS, ECR) is written directly, so the policies are readable in this repository.
3. **One spot node group per availability zone.** Kafka and Prometheus keep data on EBS volumes, which belong to one zone. A per-zone group guarantees that a replacement node appears in the zone where the volume is. Several xlarge instance types per group make spot capacity easier to find.
4. **The Iceberg catalog on AWS is the Glue Data Catalog, through Iceberg's `GlueCatalog`.** Athena reads the same tables with no extra setup. The jobs choose the catalog with one setting (`ICEBERG_CATALOG_TYPE=rest|glue`), so the local REST catalog and Glue share all other code. Glue's Iceberg REST endpoint was the alternative; it needs SigV4 request signing configured in every client and adds nothing for this project.
5. **IRSA for every AWS call from a pod, and nothing else.** The Flink and Spark job service accounts each get a role limited to the two lakehouse buckets, the environment's KMS key and the `lakehouse` Glue database. Nodes enforce IMDSv2 with a hop limit of 1, so pods cannot fall back to the node's role. Flink's S3 plugin (Hadoop 3.3.4, AWS SDK v1) does not include web identity in its default chain, so the provider is named explicitly.
6. **One customer-managed KMS key per environment** for S3 (SSE-KMS with bucket keys, to keep KMS request costs low), Kubernetes Secrets, ECR, RDS and Athena results. Every bucket blocks public access, enforces bucket-owner object ownership and denies non-TLS requests.
7. **Account-specific values are generated, not committed.** `local/scripts/aws-values.sh` writes the ECR registry, bucket names, RDS endpoint and role ARNs from Terraform outputs into git-ignored files (`helm/values/generated/<env>.yaml`, `gitops/operators/*/generated-<env>.yaml`). The ApplicationSets layer them last, with `ignoreMissingValueFiles` so local clusters are unaffected. `gitops-push.sh` force-adds them only to the snapshot pushed to the cluster's own Gitea.
8. **Gitea on EKS too.** Argo CD on EKS pulls from an in-cluster Gitea, as on kind, so nothing is published to a hosted Git service. The Gitea admin password is random and kept in a Secret. A long-lived environment would use a hosted repository instead.
9. **RDS credentials never enter Terraform state or Git.** RDS manages the master password in Secrets Manager. Until Phase 7 adds External Secrets, `aws-up.sh` copies it into the schema registry's Secret; the value is not printed.
10. **Teardown goes through Kubernetes first.** Volumes and load balancers that Kubernetes creates are not in Terraform state. `aws-down.sh` stops Argo CD, deletes the platform namespaces, waits until every PersistentVolume is gone, then runs `terraform destroy` and checks for leftovers (EBS volumes, load balancers, VPC, cluster, RDS, state).

## **Consequences**
- A demo costs roughly $0.40 per hour while the environment is up (EKS control plane, three spot nodes, one NAT gateway, a micro RDS instance, EBS). Nothing remains after teardown except the bootstrap stack and a KMS key pending deletion for 7 days.
- One NAT gateway and single-AZ RDS trade resilience for cost. Staging and prod would use one NAT gateway per zone and Multi-AZ RDS.
- The public Kubernetes API endpoint is limited to the operator's address (`api_allowed_cidrs`). Private-only access with a bastion or VPN is the production choice.
- Buckets use `force_destroy` and ECR repositories `force_delete`, which is right for a demo and wrong for anything holding real data.
- The Argo CD and Gitea bootstrap is the same script on kind and EKS; only the kube context, the environment name and the cluster labels differ.
