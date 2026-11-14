#!/usr/bin/env bash
# Bootstrap GitOps (kind, or EKS via aws-up.sh): Argo CD and Gitea, publish, register the cluster, root app. Idempotent.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require kubectl helm git

BOOT="${REPO_ROOT}/gitops/bootstrap"
GITEA_PASSWORD="$(gitea_password)"

log "Argo CD ${ARGOCD_CHART_VERSION} (${KUBE_CONTEXT})"
hc repo add argo https://argoproj.github.io/argo-helm >/dev/null 2>&1 || true
hc repo update argo >/dev/null
for image in $(hc template argocd argo/argo-cd --version "${ARGOCD_CHART_VERSION}" -f "${BOOT}/argocd-values.yaml" \
                 | grep -E '^\s+image:' | awk '{print $2}' | tr -d '"' | sort -u); do
  preload_image "${image}"
done
hc upgrade --install argocd argo/argo-cd --version "${ARGOCD_CHART_VERSION}" \
  --namespace argocd --create-namespace -f "${BOOT}/argocd-values.yaml" --wait --timeout 10m
# Third-party: Pod Security is audited and warned at "restricted", not enforced.
kc label namespace argocd pod-security.kubernetes.io/audit=restricted pod-security.kubernetes.io/warn=restricted --overwrite >/dev/null

log "Gitea (in-cluster Git server)"
preload_image "${GITEA_IMAGE}"
kc apply -f "${BOOT}/gitea.yaml"
kc -n gitops rollout status deployment/gitea --timeout=300s
kc -n gitops exec deploy/gitea -- gitea admin user create --admin --username "${GITEA_USER}" \
  --password "${GITEA_PASSWORD}" --email "${GITEA_USER}@lakehouse.local" --must-change-password=false \
  >/dev/null 2>&1 && ok "Gitea admin user created" || ok "Gitea admin user already exists"

log "Publish the working tree to Gitea"
bash "${LOCAL_DIR}/scripts/gitops-push.sh" "bootstrap"

log "Register this cluster (env ${GITOPS_ENV}) and apply the root application"
if is_kind; then
  kc apply -f "${BOOT}/cluster-local.yaml"
else
  # Same shape as cluster-local.yaml: Argo CD manages the cluster it runs in, labelled for this env.
  sed -e "s/cluster-local/cluster-${GITOPS_ENV}/" \
      -e "s/lakehouse.io\/env: local/lakehouse.io\/env: ${GITOPS_ENV}/" \
      -e "s/lakehouse.io\/env-group: local/lakehouse.io\/env-group: aws/" \
      -e "s/name: local$/name: ${GITOPS_ENV}/" "${BOOT}/cluster-local.yaml" | kc apply -f -
fi
kc apply -f "${BOOT}/projects.yaml"      # root's own project, and the locked-down default project
kc apply -f "${BOOT}/root-app.yaml"

if ! is_kind; then
  # On EKS ai-ops cannot start without these Secrets (kind creates them in deploy-ai-ops.sh).
  bash "${LOCAL_DIR}/scripts/aws-ai-secrets.sh" "${GITOPS_ENV}"     || echo "  ai-ops secrets failed: run make aws-agentcore; platform-${GITOPS_ENV} stays Degraded until they exist"
fi

log "Waiting for Argo CD to reconcile (operators, then the platform)"
gitops_wait root 300
for app in kube-prometheus-stack strimzi flink-operator spark-operator; do
  gitops_wait "${app}-${GITOPS_ENV}" 900
done
gitops_wait "${GITOPS_APP}" 1800

if is_kind; then
  ARGO_PASSWORD="$(kc -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' | base64 -d)"
  log "GitOps is live. Argo CD UI: make local-ports, then http://localhost:8090 (admin / ${ARGO_PASSWORD})"
else
  log "GitOps is live on ${KUBE_CONTEXT}. Argo CD admin password: kubectl --context ${KUBE_CONTEXT} -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' | base64 -d"
fi
