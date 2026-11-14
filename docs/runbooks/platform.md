# **Runbook: platform control plane (Argo CD, External Secrets)**

Alerts from the `platform` group in `helm/charts/observability/rules/lakehouse.yaml`.

## **ArgoCDAppOutOfSync**

**Meaning:** an application has differed from Git for 15 minutes.
**Impact:** the application's desired state is not applied; it may be running outdated or incorrect resources.
**Common causes:**

- **A failing sync keeps retrying an old revision.** A newer commit is not applied until the operation ends. Look at the application's operation message: `kc -n argocd get application <app> -o jsonpath='{.status.operationState.message}'`. Terminate it: `kc -n argocd patch application <app> --type merge -p '{"status":{"operationState":{"phase":"Terminating"}}}'`.
- **Automated sync was paused on purpose** (`gitops_pause` during a test). Resume it with `gitops_resume`.
- **A project denies a resource kind** ("not permitted in project"): add the kind to the project in `gitops/argocd/project.yaml` if it belongs there.

## **ArgoCDAppDegraded**

**Meaning:** an application's resources are unhealthy for 10 minutes.
**Impact:** the application may not function correctly until the resources are healthy again.
**Diagnose:** `kc -n argocd get application <app> -o json | jq '.status.resources[] | select(.health.status != "Healthy")'`, then the resource itself (pods, events). Admission rejections from Pod Security or Kyverno show as events on the owning ReplicaSet or StatefulSet.

## **ExternalSecretNotReady**

**Meaning:** an ExternalSecret has not synced for 10 minutes (AWS only).
**Impact:** the Kubernetes Secret keeps its last value; after an RDS password rotation the schema registry would lose its database connection.
**Diagnose:** `kc -n <ns> describe externalsecret <name>`: usually the IAM role (IRSA) lacks access to the secret, or the secret was deleted or renamed in Secrets Manager.
**Fix:** restore access or the secret; External Secrets retries on its own.
