#!/usr/bin/env bash
# Create Langfuse's Secrets and database (only when missing) and check the deployment.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require kubectl

rand() { head -c 64 /dev/urandom | base64 | tr -d '/+=\n' | head -c "$1"; }
hex() { head -c "$1" /dev/urandom | od -An -tx1 | tr -d ' \n'; }

log "Secrets"
kc get namespace langfuse >/dev/null 2>&1 || kc create namespace langfuse >/dev/null
if ! kc -n langfuse get secret langfuse-secrets >/dev/null 2>&1; then
  kc -n langfuse create secret generic langfuse-secrets \
    --from-literal=SALT="$(rand 32)" --from-literal=ENCRYPTION_KEY="$(hex 32)" \
    --from-literal=NEXTAUTH_SECRET="$(rand 40)" --from-literal=CLICKHOUSE_PASSWORD="$(rand 32)" \
    --from-literal=PUBLIC_KEY="pk-lf-$(rand 24)" --from-literal=SECRET_KEY="sk-lf-$(rand 40)" \
    --from-literal=ADMIN_PASSWORD="$(rand 20)" >/dev/null
  ok "created langfuse-secrets"
else
  ok "langfuse-secrets exists"
fi
kc -n langfuse get secret langfuse-redis-auth >/dev/null 2>&1 \
  || { kc -n langfuse create secret generic langfuse-redis-auth --from-literal=default="$(rand 32)" >/dev/null; ok "created langfuse-redis-auth"; }
for s in postgres-credentials s3-credentials; do
  kc -n data get secret "$s" -o json \
    | jq '{apiVersion, kind, type, data, metadata: {name: .metadata.name, namespace: "langfuse"}}' \
    | kc apply -f - >/dev/null
  ok "copied ${s} into langfuse"
done

log "Database"
EXISTS="$(kc -n data exec statefulset/postgres -- psql -U platform -d postgres -tAc \
  "select 1 from pg_database where datname = 'langfuse'" | tr -d '\r')"
[ "${EXISTS}" = "1" ] && ok "database langfuse exists" || {
  kc -n data exec statefulset/postgres -- psql -U platform -d postgres -c "create database langfuse" >/dev/null
  ok "created database langfuse"; }

log "Keys for ops-agent"
secret_val() { kc -n langfuse get secret langfuse-secrets -o jsonpath="{.data.$1}" | base64 -d; }
mkdir -p "${REPO_ROOT}/build"
TMP="$(mktemp -p "${REPO_ROOT}/build" langfuse.XXXXXX)"
trap 'rm -f "${TMP}"' EXIT
chmod 600 "${TMP}"
printf 'LANGFUSE_PUBLIC_KEY=%s\nLANGFUSE_SECRET_KEY=%s\nLANGFUSE_BASE_URL=http://langfuse-web.langfuse.svc:3000\n' \
  "$(secret_val PUBLIC_KEY)" "$(secret_val SECRET_KEY)" > "${TMP}"
kc -n ai-ops create secret generic langfuse-api --from-env-file="${TMP}" --dry-run=client -o yaml | kc apply -f - >/dev/null
ok "ai-ops/langfuse-api"

log "Status"
kc -n langfuse get pods 2>/dev/null || true
echo
echo "UI: make local-ports, then http://localhost:3001  (user admin@lakehouse.local; password:"
echo "    kubectl -n langfuse get secret langfuse-secrets -o jsonpath='{.data.ADMIN_PASSWORD}' | base64 -d )"
