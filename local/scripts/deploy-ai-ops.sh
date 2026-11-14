#!/usr/bin/env bash
# Build the ops agent images, load them into kind, create the missing Secrets and restart the workloads.
#   usage: deploy-ai-ops.sh [--build-only | --secrets-only]    (--secrets-only: AWS, where Argo CD deploys ECR images)
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require docker kind kubectl

APPS="$(cd "${LOCAL_DIR}/../apps" && pwd)"
NS=ai-ops

if [ "${1:-}" != "--secrets-only" ]; then
log "Unit tests and images"
bash "${APPS}/mcp-platform-tools/run-tests.sh" -q | tail -1
bash "${APPS}/ai-ops/run-tests.sh" -q | tail -1
(cd "${APPS}/mcp-platform-tools" && docker_build --build-context runbooks=../../docs/runbooks \
  -t lakehouse/mcp-platform-tools:dev .) >/dev/null
(cd "${APPS}/ai-ops" && docker_build -t lakehouse/ai-ops:dev .) >/dev/null
(cd "${APPS}/hermes" && docker_build -t lakehouse/hermes:dev .) >/dev/null
preload_image lakehouse/mcp-platform-tools:dev force
preload_image lakehouse/ai-ops:dev force
preload_image lakehouse/hermes:dev force

[ "${1:-}" = "--build-only" ] && exit 0
fi

log "Secrets"
kc get namespace "${NS}" >/dev/null 2>&1 || { echo "namespace ${NS} missing: run make gitops-push first" >&2; exit 1; }
if ! kc -n "${NS}" get secret ai-ops-internal >/dev/null 2>&1; then
  rand() { head -c 48 /dev/urandom | base64 | tr -d '/+=\n' | head -c "$1"; }
  kc -n "${NS}" create secret generic ai-ops-internal \
    --from-literal=MCP_TOKEN="$(rand 40)" --from-literal=CONTROL_TOKEN="$(rand 40)" \
    --from-literal=ACTION_SIGNING_KEY="$(rand 48)" >/dev/null
  ok "created ai-ops-internal (random tokens and signing key)"
else
  ok "ai-ops-internal exists"
fi
SLACK_ENV="${LOCAL_DIR}/secrets/slack.env"
if [ -f "${SLACK_ENV}" ]; then
  kc -n "${NS}" create secret generic ops-slack --from-env-file="${SLACK_ENV}" --dry-run=client -o yaml \
    | kc apply -f - >/dev/null
  ok "ops-slack from local/secrets/slack.env"
else
  ok "no local/secrets/slack.env: approvals through the CLI only"
fi
kc -n "${NS}" get secret bedrock-credentials >/dev/null 2>&1 \
  && ok "bedrock-credentials exists (refresh: bash local/scripts/ai-creds.sh)" \
  || ok "no bedrock-credentials yet: bash local/scripts/ai-creds.sh"

if [ "${1:-}" = "--secrets-only" ]; then
  # Pods waiting for a missing Secret retry on their own; --secrets-only never changes an existing one.
  exit 0
fi

log "Rolling out"
# The local tag dev does not change, so a restart picks up the new images and Secret values.
for d in mcp-platform-tools ops-control ops-agent hermes; do
  kc -n "${NS}" rollout restart "deployment/${d}" >/dev/null 2>&1 || true
done
for d in mcp-platform-tools ops-control ops-agent hermes; do
  kc -n "${NS}" rollout status "deployment/${d}" --timeout=180s 2>/dev/null | tail -1 || true
done
