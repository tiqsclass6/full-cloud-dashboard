#!/usr/bin/env bash
# Replay every eval scenario through the real model and score the answers (costs tokens).
#   usage: ai-eval.sh [runs per scenario] [scenario ...]
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require aws docker jq terraform
export MSYS_NO_PATHCONV=1

RUNS="${1:-1}"; shift || true
SCENARIOS=(); for s in "$@"; do SCENARIOS+=(--scenario "$s"); done
ROLE_ARN="${OPS_AGENT_ROLE_ARN:-$(terraform -chdir="${REPO_ROOT}/terraform/bootstrap" output -raw ops_agent_local_role_arn | tr -d '\r')}"
OUT="results/$(date -u +%Y-%m-%dT%H%M)"

mkdir -p "${REPO_ROOT}/build"
ENV_FILE="$(mktemp -p "${REPO_ROOT}/build" eval-env.XXXXXX)"
trap 'rm -f "${ENV_FILE}"' EXIT
chmod 600 "${ENV_FILE}"
aws sts assume-role --role-arn "${ROLE_ARN}" --role-session-name "ops-agent-eval-$(date +%s)" --duration-seconds 3600 \
  --output json | jq -r '.Credentials | "AWS_ACCESS_KEY_ID=\(.AccessKeyId)\nAWS_SECRET_ACCESS_KEY=\(.SecretAccessKey)\nAWS_SESSION_TOKEN=\(.SessionToken)"' \
  > "${ENV_FILE}"

log "Eval: ${RUNS} run(s) per scenario -> evals/agent-scenarios/${OUT}"
docker run --rm --env-file "${ENV_FILE}" -e AWS_REGION=us-east-1 \
  -v "${REPO_ROOT}/evals:/evals" -w /evals \
  lakehouse/ai-ops:dev eval --root /evals/agent-scenarios --runs "${RUNS}" --out "/evals/agent-scenarios/${OUT}" "${SCENARIOS[@]}"
