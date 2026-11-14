# **Helm charts**

The whole platform as Helm charts, deployed by Argo CD ([**gitops/**](../gitops/README.md)). Design reasons are in [**ADR 0006**](../docs/architecture-decisions/0006-helm-and-gitops.md).

## **Charts**

| **Chart**               | **What it deploys**                                                                                             | **Namespace**         |
| ----------------------- | --------------------------------------------------------------------------------------------------------------- | --------------------- |
| **`lakehouse-lib`**     | Library only: labels, image references, security contexts, S3 and Iceberg env vars                              | n/a                   |
| **`ingestion-service`** | Deployment, Service, ServiceMonitor, PodDisruptionBudget, optional HPA                                          | `apps`                |
| **`flink-jobs`**        | The Kafka-to-Iceberg `FlinkDeployment`                                                                          | `apps`                |
| **`spark-jobs`**        | One `ScheduledSparkApplication` per entry in `schedules`                                                        | `apps`                |
| **`kafka-cluster`**     | Strimzi `Kafka`, `KafkaNodePool`s, `KafkaTopic`s, PodMonitors                                                   | `kafka`, `monitoring` |
| **`schema-registry`**   | Apicurio Registry (Deployment, Service)                                                                         | `data`                |
| **`local-data`**        | Local stand-ins for AWS services: Postgres, SeaweedFS (S3), Iceberg REST catalog, toolbox; never enabled on AWS | `data`, `apps`        |
| **`platform`**          | Umbrella: the namespaces plus all of the above, each behind `<chart>.enabled`                                   | all                   |

## **Environments and value layering**

Values are applied in this order, later files overriding earlier ones:

| **Environment**  | **Files after the chart defaults**       |
| ---------------- | ---------------------------------------- |
| **local (kind)** | `values/local.yaml`                      |
| **dev**          | `values/aws.yaml`, `values/dev.yaml`     |
| **staging**      | `values/aws.yaml`, `values/staging.yaml` |
| **prod**         | `values/aws.yaml`, `values/prod.yaml`    |

What changes between them:

| **Setting**                        | **local**                                                                      | **dev**                   | **staging**               | **prod**                  |
| ---------------------------------- | ------------------------------------------------------------------------------ | ------------------------- | ------------------------- | ------------------------- |
| **Images**                         | `dev`, loaded into kind                                                        | ECR, tag `0.1.0`          | ECR, tag `0.1.0`          | ECR, tag `0.1.0`          |
| **S3**                             | SeaweedFS endpoint + static dev keys                                           | real S3, IRSA             | real S3, IRSA             | real S3, IRSA             |
| **Iceberg catalog**                | in-cluster REST on Postgres                                                    | Glue Iceberg REST         | Glue Iceberg REST         | Glue Iceberg REST         |
| **Kafka nodes**                    | 3 combined controller/broker                                                   | 3 controllers + 3 brokers | 3 controllers + 3 brokers | 3 controllers + 6 brokers |
| **`events.raw` partitions**        | 6                                                                              | 6                         | 6                         | 12                        |
| **Ingestion replicas**             | 2 (fixed)                                                                      | HPA 2-4                   | HPA 2-6                   | HPA 3-12, PDB min 2       |
| **Flink state backend**            | hash map, full snapshots                                                       | RocksDB, incremental      | RocksDB, incremental      | RocksDB, incremental      |
| **Flink parallelism**              | 2                                                                              | 2                         | 4                         | 6                         |
| **Flink HA**                       | on (needed for safe recovery, see [**gitops/README.md**](../gitops/README.md)) | on                        | on                        | on                        |
| **Spark schedules**                | suspended                                                                      | running                   | running                   | running                   |
| **Local stand-ins (`local-data`)** | on                                                                             | off                       | off                       | off                       |

The AWS overlays use placeholders until Phase 6 creates the resources: account `111122223333`, `lakehouse-<env>-checkpoints` buckets, `<rds-endpoint>`, and IAM role ARNs in `gitops/operators/*/aws.yaml`. They render and validate today but have not been deployed. The Glue catalog endpoint also needs SigV4 support in the jobs (Phase 6).

## **Working with the charts**

```bash
make helm-check             # dependencies, lint, render and schema-check all four environments
helm template platform helm/charts/platform -f helm/values/local.yaml     # see what local gets
make gitops-push            # publish to the in-cluster Gitea; Argo CD applies it within a minute
```

`make helm-check` validates against Kubernetes and CRD schemas with kubeconform when it is on the `PATH` (or `KUBECONFORM=...` is set), and `bash local/scripts/helm-check.sh --server` adds a server-side dry run against the kind cluster.

### **Rules the charts follow**

- **Names and selectors are fixed** (`app: <name>`). They match the resources created before the charts existed, which let Argo CD adopt them in place, and Deployment and StatefulSet selectors cannot change anyway.
- **Strimzi resources use labels without the chart version**, because Strimzi copies them onto the broker pods; a version label would restart every broker on each chart release.
- **When the HPA is on, the Deployment omits `replicas`**, so Argo CD and the autoscaler do not fight.
- **Library dependencies are vendored** into each chart's `charts/` folder, because Argo CD only builds the top chart's dependencies. `make gitops-push` and `make helm-check` rebuild them.
