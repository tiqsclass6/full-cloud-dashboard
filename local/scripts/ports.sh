#!/usr/bin/env bash
# Port-forward the UIs/APIs to localhost (logins: docs/lab/RUNBOOK.md); Ctrl+C stops all of them.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require kubectl

trap stop_forwards EXIT INT TERM

# Langfuse, the Iceberg REST catalog and S3 run only on kind (EKS uses Glue and Amazon S3).
fwd_if_present() {
  if kc -n "$1" get "$2" >/dev/null 2>&1; then fwd "$@"; else echo "  skip $1/$2: not on ${KUBE_CONTEXT}"; fi
}

fwd argocd svc/argocd-server 8090:80
fwd gitops svc/gitea-http 3300:3000
fwd monitoring svc/kps-grafana 3000:80
fwd monitoring svc/kps-prometheus 9090:9090
fwd monitoring svc/kps-alertmanager 9093:9093
fwd_if_present langfuse svc/langfuse-web 3001:3000
fwd data svc/apicurio-registry 8085:8080
fwd_if_present data svc/iceberg-rest 8181:8181
fwd_if_present data svc/s3 8333:8333

if pw="$(kc -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' 2>/dev/null | base64 -d 2>/dev/null)"; then
  echo "Argo CD: http://localhost:8090  user admin, password ${pw}"
fi
echo "Port-forwards running; Ctrl+C to stop."
wait
