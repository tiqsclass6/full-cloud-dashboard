# **ADR 0006: Helm charts and GitOps with Argo CD**

- Status: accepted
- Date: 2026-10-29

## **Context**
Phases 1-4 deployed everything with `kubectl apply` and `helm install` from scripts. That leaves no single declared state, no drift detection, and no clean way to express the differences between the local cluster and the AWS environments that Phase 6 adds. The project also has no Git commits or remote yet.

## **Decisions**
1. **One chart per component plus an umbrella.** `ingestion-service`, `flink-jobs`, `spark-jobs`, `kafka-cluster`, `schema-registry` and `local-data`, combined by the `platform` umbrella. Each component can be rendered and tested alone; the umbrella is the deployable unit per environment.
2. **A library chart (`lakehouse-lib`)** for labels, images, security contexts and the S3 and Iceberg environment variables, so those rules exist once.
3. **Layered overlays selected by cluster labels.** Values apply as chart defaults, then `values/<env-group>.yaml`, then `values/<env>.yaml` (local, or aws plus dev/staging/prod). An ApplicationSet reads `lakehouse.io/env` and `lakehouse.io/env-group` from the Argo CD cluster registration, so adding an EKS cluster with the right labels is all it takes to deploy an environment to it.
4. **Argo CD, app of apps.** One hand-applied `root` Application manages the project and two ApplicationSets: operators (upstream charts pinned to exact versions, values from our Git through a multi-source Application) and the platform umbrella. Automated sync with prune and self-heal everywhere.
5. **In-cluster Gitea as the local Git source.** `make gitops-push` snapshots the working tree into a separate Git directory and pushes it to Gitea. Argo CD gets real Git semantics (revisions, polling) while the project's own repository stays untouched. On AWS the source becomes a GitHub repository; only the repository URL changes.
6. **Adopt, do not recreate.** Charts keep every existing resource name and the `app: <name>` selectors (Deployment and StatefulSet selectors are immutable), so Argo CD took over the running platform in place. Kafka, Postgres and S3 data survived; pods restarted once for the new labels.
7. **Vendored chart dependencies.** Argo CD builds only the top chart's dependencies, so each component's `charts/` holds its built library dependency. `gitops-push` rebuilds them before every push.
8. **Run-time changes are explicit.**
   - Two FlinkDeployment fields that operations change on purpose (`job.state` for stop and start, `restartNonce` for restarts) are ignored by Argo CD.
   - Everything else that tests change deliberately (deleting a topic, JVM flags, a suspended schedule) happens with automated sync paused (`gitops_pause` / `gitops_resume` in `local/scripts/lib.sh`). Resuming restores Git's state.
   - The ApplicationSets ignore `syncPolicy` changes on generated Applications so a pause is not undone.
9. **Server-side apply and server-side diff.** The kube-prometheus-stack CRDs exceed the client-side annotation limit, and server-side diff avoids false "out of sync" results from fields that controllers set.
10. **Strimzi resources get labels without the chart version.** Strimzi copies a `Kafka` resource's labels to the broker pods, so a chart-version label would roll every broker on each chart release.

## **Consequences**
- Git (the snapshot) is the source of truth; a manual change is reverted within seconds unless sync is paused, and a paused application shows as OutOfSync, so the pause is visible.
- Local images keep the tag `dev`, so a new build is rolled out with a restart rather than a spec change. On AWS, images get real tags and a tag change in Git is the deployment.
- The AWS overlays render and validate today, but use placeholders (account id, buckets, RDS endpoint, role ARNs) until Phase 6 creates the resources. The jobs also need SigV4 support to use Glue's Iceberg REST endpoint (Phase 6).
- Argo CD itself and Gitea are installed by a bootstrap script (Helm and one manifest), not from Git; that is the usual bootstrap boundary.
- The AppProject is broad (all namespaces and cluster resources) for now; Phase 7 narrows it.
- Adopting a FAILED Flink job exposed that recovery without Flink HA restores the last savepoint, not the latest checkpoint, and duplicated 500,000 rows. Flink HA is now on in every environment and the chart refuses `restartFailedJob` without it (incident write-up in [**gitops/README.md**](../../gitops/README.md)).
- On an empty cluster, resources need an order (namespaces, then storage and catalog, then buckets, then workloads). The charts declare it with Argo CD sync waves; without them Flink crash-looped until its bucket existed (see gitops/README.md).
