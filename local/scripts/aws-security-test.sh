#!/usr/bin/env bash
# Security checks on AWS (after aws-smoke.sh): image signatures, synced secrets, RDS TLS, audit services, cluster suites.
#   usage: aws-security-test.sh [env]     (default dev)
ENV_NAME="${1:-dev}"
GITOPS_ENV="${ENV_NAME}" KUBE_CONTEXT="lakehouse-${ENV_NAME}"
export GITOPS_ENV KUBE_CONTEXT
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require aws terraform kubectl docker jq
FAILED=0

TF_DIR="${REPO_ROOT}/terraform/envs/${ENV_NAME}"
tfout() { terraform -chdir="${TF_DIR}" output -raw "$1"; }
REGION="$(tfout region)"
REGISTRY="$(tfout ecr_registry)"
TAG="$(grep -E '^\s+imageTag:' "${REPO_ROOT}/helm/values/${ENV_NAME}.yaml" | head -1 | awk '{print $2}' | tr -d '"')"
NAME="lakehouse-${ENV_NAME}"

pod_dry_run() {  # pod_dry_run <image>: server-side dry run of a compliant pod in apps; prints errors
  kc apply --dry-run=server -f - 2>&1 >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata: {name: signature-test, namespace: apps}
spec:
  securityContext: {runAsNonRoot: true, runAsUser: 10001, seccompProfile: {type: RuntimeDefault}}
  containers:
    - name: c
      image: $1
      command: ["sleep", "60"]
      resources: {requests: {cpu: 10m, memory: 32Mi}, limits: {memory: 64Mi}}
      securityContext: {allowPrivilegeEscalation: false, capabilities: {drop: ["ALL"]}}
EOF
}

log "Image signatures (Kyverno verify-image-signatures)"
signed="${REGISTRY}/lakehouse/ingestion-service:${TAG}"
err="$(pod_dry_run "${signed}")" || true
[ -z "${err}" ] && ok "signed image admitted (${signed##*/})" || { fail "signed image refused"; echo "        ${err:0:300}"; }
# An image the platform never signed, in the platform's own repository: a new digest (one extra label).
unsigned="${REGISTRY}/lakehouse/ingestion-service:unsigned-test"
aws ecr get-login-password --region "${REGION}" | docker login --username AWS --password-stdin "${REGISTRY}" >/dev/null
printf 'FROM %s\nLABEL lakehouse.io/test=unsigned\n' "lakehouse/ingestion-service:dev" | docker build -q -t "${unsigned}" - >/dev/null
docker push -q "${unsigned}" >/dev/null
err="$(pod_dry_run "${unsigned}")" || true   # a refusal is the expected outcome
echo "${err}" | grep -q "verify-image-signatures" && ok "unsigned image refused" \
  || { fail "unsigned image not refused by the signature policy"; echo "        ${err:0:300}"; }
aws ecr batch-delete-image --region "${REGION}" --repository-name lakehouse/ingestion-service \
  --image-ids imageTag=unsigned-test >/dev/null
docker image rm "${unsigned}" >/dev/null 2>&1 || true

log "Secrets from AWS Secrets Manager (External Secrets)"
for es in data/schema-registry-db monitoring/grafana-admin; do
  ready="$(kc -n "${es%/*}" get externalsecret "${es#*/}" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)"
  keys="$(kc -n "${es%/*}" get secret "${es#*/}" -o json 2>/dev/null | jq -r '.data | keys | join(",")')"
  [ "${ready}" = True ] && ok "${es} synced (keys: ${keys})" || fail "${es} not synced (Ready=${ready:-missing})"
done

log "Data in transit: RDS"
force_ssl="$(aws rds describe-db-parameters --region "${REGION}" --db-parameter-group-name "${NAME}-postgres17" \
  --query "Parameters[?ParameterName=='rds.force_ssl'].ParameterValue" --output text)"
[ "${force_ssl}" = 1 ] && ok "rds.force_ssl = 1 (non-TLS connections refused)" || fail "rds.force_ssl = ${force_ssl}"
apicurio="$(kc -n data get deploy apicurio-registry -o jsonpath='{.status.readyReplicas}')"
[ "${apicurio:-0}" -ge 1 ] && ok "schema registry connected over TLS (${apicurio} ready)" || fail "schema registry not ready"

log "Audit"
logging="$(aws cloudtrail get-trail-status --region "${REGION}" --name cloud-dashboard-account --query IsLogging --output text)"
[ "${logging}" = True ] && ok "CloudTrail logging" || fail "CloudTrail not logging"
created="$(aws cloudtrail lookup-events --region "${REGION}" --max-results 1 \
  --lookup-attributes AttributeKey=ResourceName,AttributeValue="${NAME}" --query 'length(Events)' --output text)"
[ "${created:-0}" -ge 1 ] && ok "CloudTrail recorded API calls on ${NAME}" || fail "no CloudTrail events for ${NAME}"
recording="$(aws configservice describe-configuration-recorder-status --region "${REGION}" \
  --query 'ConfigurationRecordersStatus[0].recording' --output text)"
[ "${recording}" = True ] && ok "AWS Config recording" || fail "AWS Config not recording"
aws configservice describe-compliance-by-config-rule --region "${REGION}" \
  --config-rule-names $(aws configservice describe-config-rules --region "${REGION}" \
    --query "ConfigRules[?starts_with(ConfigRuleName, '${NAME}-')].ConfigRuleName" --output text) \
  --query 'ComplianceByConfigRules[].[ConfigRuleName,Compliance.ComplianceType]' --output text \
  | sed "s/^${NAME}-//" | awk '{printf "        %-36s %s\n", $1, $2}'
analyzer="$(aws accessanalyzer list-analyzers --region "${REGION}" --query "analyzers[?name=='cloud-dashboard-account'].status" --output text)"
[ "${analyzer}" = ACTIVE ] && ok "IAM Access Analyzer active" || fail "Access Analyzer status: ${analyzer:-missing}"

log "Cluster suites on ${KUBE_CONTEXT}"
for suite in kafka-security-test network-policy-test policy-test; do
  if bash "${LOCAL_DIR}/scripts/${suite}.sh" >"${REPO_ROOT}/build/security/${suite}-${ENV_NAME}.log" 2>&1; then
    ok "${suite}: $(sed 's/\x1b\[[0-9;]*m//g' "${REPO_ROOT}/build/security/${suite}-${ENV_NAME}.log" | grep -c '^  ok') checks passed"
  else
    fail "${suite} (build/security/${suite}-${ENV_NAME}.log)"; grep -E "FAIL" "${REPO_ROOT}/build/security/${suite}-${ENV_NAME}.log" | head -5
  fi
done

echo
if [ "${FAILED}" -eq 0 ]; then log "All AWS security checks passed (${ENV_NAME})."; else log "Some AWS security checks FAILED"; exit 1; fi
