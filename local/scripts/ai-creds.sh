#!/usr/bin/env bash
# Short-lived Bedrock credentials (role from terraform/bootstrap) in the Secret ai-ops/bedrock-credentials.
#   usage: ai-creds.sh [seconds]      default 3600 (the role allows up to 4 hours)
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require aws kubectl jq terraform

SECONDS_VALID="${1:-3600}"
ROLE_ARN="${OPS_AGENT_ROLE_ARN:-$(terraform -chdir="${REPO_ROOT}/terraform/bootstrap" output -raw ops_agent_local_role_arn | tr -d '\r')}"

log "Assuming ${ROLE_ARN##*/} for ${SECONDS_VALID}s"
CREDS="$(aws sts assume-role --role-arn "${ROLE_ARN}" --role-session-name "ops-agent-kind-$(date +%s)" \
  --duration-seconds "${SECONDS_VALID}" --output json)"
EXPIRES="$(printf '%s' "${CREDS}" | jq -r .Credentials.Expiration)"

# Written through a private temporary file (removed on exit) so the values never appear on a command line.
mkdir -p "${REPO_ROOT}/build"
TMP="$(mktemp -p "${REPO_ROOT}/build" creds.XXXXXX)"      # build/ (git-ignored); native tools cannot read MSYS /tmp
trap 'rm -f "${TMP}"' EXIT
chmod 600 "${TMP}"
printf '%s' "${CREDS}" | jq -r '.Credentials | "AWS_ACCESS_KEY_ID=\(.AccessKeyId)\nAWS_SECRET_ACCESS_KEY=\(.SecretAccessKey)\nAWS_SESSION_TOKEN=\(.SessionToken)"' > "${TMP}"
kc -n ai-ops create secret generic bedrock-credentials --from-env-file="${TMP}" --dry-run=client -o yaml \
  | kc apply -f - >/dev/null
kc -n ai-ops annotate secret bedrock-credentials --overwrite lakehouse.io/expires="${EXPIRES}" >/dev/null
ok "bedrock-credentials valid until ${EXPIRES}"

kc -n ai-ops rollout restart deployment/ops-agent >/dev/null 2>&1 && ok "ops-agent restarted" || true
