#!/usr/bin/env bash
# Create the ai-ops Secrets (internal tokens; MCP TLS from a throwaway CA) once the namespace exists. Idempotent.
#   usage: aws-ai-secrets.sh [env]     (default dev)
ENV_NAME="${1:-dev}"
GITOPS_ENV="${ENV_NAME}" KUBE_CONTEXT="lakehouse-${ENV_NAME}"
export GITOPS_ENV KUBE_CONTEXT
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require aws terraform kubectl openssl
export MSYS_NO_PATHCONV=1

TF_DIR="${REPO_ROOT}/terraform/envs/${ENV_NAME}"
tfout() { terraform -chdir="${TF_DIR}" output -raw "$1" | tr -d '\r'; }
REGION="$(tfout region)"
HOSTNAME_TLS="$(grep -A4 'vpcEndpoint:' "${REPO_ROOT}/helm/charts/ai-ops/values.yaml" | awk '/hostname:/{print $2}')"

WORK="${REPO_ROOT}/build/agentcore-${ENV_NAME}"
mkdir -p "${WORK}"; chmod 700 "${WORK}"
trap 'rm -f "${WORK}"/*.key' EXIT

log "Waiting for the ai-ops namespace (created by Argo CD)"
for _ in $(seq 1 180); do
  kc get namespace ai-ops >/dev/null 2>&1 && break
  sleep 10
done
kc get namespace ai-ops >/dev/null 2>&1 || { fail "namespace ai-ops did not appear within 30 minutes"; exit 1; }
ok "ai-ops namespace exists"

log "Shared secrets (ai-ops-internal: MCP and control tokens, signing key)"
bash "${LOCAL_DIR}/scripts/deploy-ai-ops.sh" --secrets-only | grep -E "ok|FAIL" | head -3

log "TLS for ${HOSTNAME_TLS}"
if kc -n ai-ops get secret mcp-tls >/dev/null 2>&1; then
  ok "ai-ops/mcp-tls exists"
else
  # A minimal config of our own: some OpenSSL builds (Git for Windows) ship a default with invalid values.
  printf '[req]\ndistinguished_name = dn\nprompt = no\n[dn]\nCN = unused\n' > "${WORK}/openssl.cnf"
  openssl req -config "${WORK}/openssl.cnf" -x509 -newkey rsa:3072 -nodes -days 30 -subj "/CN=lakehouse ai-ops MCP CA" \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -keyout "${WORK}/ca.key" -out "${WORK}/ca.crt" 2>/dev/null
  openssl req -config "${WORK}/openssl.cnf" -newkey rsa:3072 -nodes -subj "/CN=${HOSTNAME_TLS}" \
    -keyout "${WORK}/server.key" -out "${WORK}/server.csr" 2>/dev/null
  printf 'subjectAltName=DNS:%s\nextendedKeyUsage=serverAuth\nbasicConstraints=CA:FALSE\n' "${HOSTNAME_TLS}" > "${WORK}/server.ext"
  openssl x509 -req -in "${WORK}/server.csr" -CA "${WORK}/ca.crt" -CAkey "${WORK}/ca.key" -CAcreateserial -days 30 \
    -extfile "${WORK}/server.ext" -out "${WORK}/server.crt" 2>/dev/null
  rm -f "${WORK}/ca.key"                         # nothing else can ever be signed by this CA
  kc -n ai-ops create secret tls mcp-tls --cert="${WORK}/server.crt" --key="${WORK}/server.key" >/dev/null
  aws secretsmanager put-secret-value --region "${REGION}" --secret-id "$(tfout mcp_ca_secret_arn)" \
    --secret-string "file://${WORK}/ca.crt" >/dev/null
  kc -n ai-ops rollout restart deployment/mcp-platform-tools >/dev/null 2>&1 || true
  ok "certificate for ${HOSTNAME_TLS} (30 days); CA certificate in Secrets Manager; CA key deleted"
fi
