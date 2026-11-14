#!/usr/bin/env bash
# Tear an AWS environment down (volumes and load balancers through Kubernetes first) and check nothing billable is left.
#   usage: aws-down.sh [env]     PURGE_DEV_IMAGES=1 also removes the newest lakehouse/<app>:dev images
ENV_NAME="${1:-dev}"
GITOPS_ENV="${ENV_NAME}" KUBE_CONTEXT="lakehouse-${ENV_NAME}"
export GITOPS_ENV KUBE_CONTEXT
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require aws terraform kubectl
FAILED=0

TF_DIR="${REPO_ROOT}/terraform/envs/${ENV_NAME}"
# After a partly failed destroy, output -raw prints nothing and exits 0: do not rely on its exit status.
REGION="$(terraform -chdir="${TF_DIR}" output -raw region 2>/dev/null | tr -d '\r' || true)"
REGION="${REGION:-us-east-1}"
CLUSTER="lakehouse-${ENV_NAME}"

# Tags of this project's images that are not the newest build of their app (groups: main, arm64, test).
stale_project_tags() {
  local apps='(ingestion-service|flink-iceberg-job|spark-jobs|iceberg-exporter|mcp-platform-tools|ai-ops|hermes)' t k lines="" keep=""
  for t in $(docker images --format '{{.Repository}}:{{.Tag}}' | tr -d '\r' \
              | grep -E "^([0-9]+\.dkr\.ecr\.[a-z0-9-]+\.amazonaws\.com/)?lakehouse/${apps}:" || true); do
    case "${t}" in *:test) k=test ;; *arm64*) k=arm64 ;; *) k=main ;; esac
    lines="${lines}$(docker image inspect --format "{{.Id}}|${k}|{{.Created}}|${t}" "${t}" | tr -d '\r')"$'\n'
  done
  if [ "${PURGE_DEV_IMAGES:-0}" != 1 ]; then
    keep="$(printf '%s' "${lines}" | awk -F'|' '$2 != "test" {a = $4; sub(/.*lakehouse\//, "", a); sub(/:.*/, "", a); print a "|" $2 "|" $3 "|" $1}' \
            | sort -t'|' -k1,2 -k3,3r | awk -F'|' '!seen[$1 "|" $2]++ {print $4}')"
  fi
  printf '%s' "${lines}" | KEEP="${keep}" awk -F'|' 'BEGIN {n = split(ENVIRON["KEEP"], a, "\n"); for (i = 1; i <= n; i++) k[a[i]] = 1}
    $2 == "test" || !($1 in k) {print $4}'
}

bash "${LOCAL_DIR}/scripts/aws-agentcore.sh" "${ENV_NAME}" down || echo "  (AgentCore target cleanup failed; terraform destroy may report the gateway in use)"

if kc get nodes >/dev/null 2>&1; then
  log "Stopping Argo CD"
  kc -n argocd scale deployment,statefulset --all --replicas=0 >/dev/null 2>&1 || true

  log "Deleting workloads and their volumes"
  # Custom resources first, while their operators still run and can clear their finalizers.
  kc -n apps delete flinkdeployments --all --wait=true --timeout=180s >/dev/null 2>&1 || true
  kc -n apps delete scheduledsparkapplications,sparkapplications --all --wait=true --timeout=120s >/dev/null 2>&1 || true
  kc -n kafka delete kafkas,kafkanodepools,kafkatopics --all --wait=true --timeout=300s >/dev/null 2>&1 || true
  kc delete services -A --field-selector spec.type=LoadBalancer --wait=true >/dev/null 2>&1 || true
  for ns in apps data kafka ai-ops monitoring observability gitops flink-operator spark-operator argocd; do
    kc delete namespace "${ns}" --wait=false --ignore-not-found >/dev/null
  done
  for _ in $(seq 1 90); do
    left="$(kc get pv --no-headers 2>/dev/null | wc -l)"
    [ "${left}" -eq 0 ] && break
    sleep 10
  done
  [ "${left}" -eq 0 ] && ok "all PersistentVolumes released" || fail "${left} PersistentVolume(s) still present"
else
  echo "cluster ${KUBE_CONTEXT} not reachable; skipping Kubernetes cleanup"
fi

log "terraform destroy (${ENV_NAME})"
terraform -chdir="${TF_DIR}" destroy -auto-approve -input=false -no-color \
  -var-file=terraform.tfvars | grep -E "Destroy complete|Error" || true
# AgentCore Runtime releases its network interfaces late: wait for them (up to 60 minutes), then destroy the rest.
if [ -n "$(terraform -chdir="${TF_DIR}" state list 2>/dev/null)" ]; then
  vpc="$(aws ec2 describe-vpcs --region "${REGION}" --filters "Name=tag:Name,Values=${CLUSTER}" --query 'Vpcs[0].VpcId' --output text)"
  for _ in $(seq 1 60); do
    [ "$(aws ec2 describe-network-interfaces --region "${REGION}" --filters "Name=vpc-id,Values=${vpc}" \
          --query 'length(NetworkInterfaces)' --output text | tr -d '\r')" = 0 ] && break
    sleep 60
  done
  terraform -chdir="${TF_DIR}" destroy -auto-approve -input=false -no-color \
    -var-file=terraform.tfvars | grep -E "Destroy complete|Error" || true
fi

log "CloudWatch log groups"
# EKS recreates its log group during deletion and AgentCore adds per-Runtime ones: delete them once the cluster is gone.
AGENT_LOGS="/aws/bedrock-agentcore/runtimes/${CLUSTER//-/_}_ops_agent-"
if [ -z "$(aws eks list-clusters --region "${REGION}" --query "clusters[?@=='${CLUSTER}']" --output text)" ]; then
  for lg in "/aws/eks/${CLUSTER}/cluster" \
            $(aws logs describe-log-groups --region "${REGION}" --log-group-name-prefix "${AGENT_LOGS}" \
                --query 'logGroups[].logGroupName' --output text | tr '\t' ' ' | tr -d '\r'); do
    aws logs delete-log-group --region "${REGION}" --log-group-name "${lg}" >/dev/null 2>&1 && ok "deleted log group ${lg}"
  done
else
  echo "cluster ${CLUSTER} still exists; leaving its log groups"
fi

log "Local Docker images from aws-up"
# Keep the newest build of each app image (and the arm64 agent); remove older ones. Never forced, so used images stay.
if docker info >/dev/null 2>&1; then
  n_tags=0
  for t in $(stale_project_tags); do docker rmi "${t}" >/dev/null 2>&1 && n_tags=$((n_tags + 1)); done
  # Untagged builds of our apps (picked by entrypoint; nothing references them)
  n_old=0
  for id in $(docker images -f dangling=true -q); do
    case "$(docker image inspect --format '{{.Config.Entrypoint}} {{.Config.Cmd}}' "${id}" 2>/dev/null)" in
      *ingestion-service*|*iceberg-exporter*|*platform_tools.server*|*"python -m ops_agent"*|*hermes*|*/opt/entrypoint.sh*|*docker-entrypoint.sh*help*)
        docker rmi "${id}" >/dev/null 2>&1 && n_old=$((n_old + 1)) ;;
    esac
  done
  ok "kept the newest build of each app; removed ${n_tags} older tag(s) and ${n_old} untagged image(s)"
else
  echo "Docker is not running; skipping the local image cleanup"
fi

log "Leftover check"
# Released volumes are deleted asynchronously; AWS has taken over an hour. Wait 5 minutes, then report the rest.
for _ in $(seq 1 30); do
  vols="$(aws ec2 describe-volumes --region "${REGION}" \
    --filters "Name=tag:kubernetes.io/cluster/${CLUSTER},Values=owned" \
    --query 'Volumes[].[VolumeId,State]' --output text | tr -d '\r')"
  { [ -z "${vols}" ] || grep -qv 'deleting$' <<<"${vols}"; } && break
  sleep 10
done
DELETING=0
if [ -z "${vols}" ]; then
  ok "no EBS volumes tagged for ${CLUSTER}"
elif grep -qv 'deleting$' <<<"${vols}"; then
  fail "EBS volumes left: $(awk '{printf "%s (%s) ", $1, $2}' <<<"${vols}")"
else
  DELETING="$(wc -l <<<"${vols}")"
  printf '\033[1;33m wait\033[0m  %s EBS volume(s) still being deleted by AWS: %s\n' "${DELETING}" "$(awk '{printf "%s ", $1}' <<<"${vols}")"
fi
lbs="$(aws elbv2 describe-load-balancers --region "${REGION}" \
  --query "LoadBalancers[?contains(LoadBalancerName, 'k8s-')].LoadBalancerName" --output text)"
[ -z "${lbs}" ] && ok "no Kubernetes load balancers" || fail "load balancers left: ${lbs}"
vpcs="$(aws ec2 describe-vpcs --region "${REGION}" --filters "Name=tag:Name,Values=${CLUSTER}" \
  --query 'Vpcs[].VpcId' --output text)"
[ -z "${vpcs}" ] && ok "VPC removed" || fail "VPC left: ${vpcs}"
eks="$(aws eks list-clusters --region "${REGION}" --query "clusters[?@=='${CLUSTER}']" --output text)"
[ -z "${eks}" ] && ok "EKS cluster removed" || fail "EKS cluster left: ${eks}"
rds="$(aws rds describe-db-instances --region "${REGION}" \
  --query "DBInstances[?DBInstanceIdentifier=='${CLUSTER}'].DBInstanceStatus" --output text)"
[ -z "${rds}" ] && ok "RDS instance removed" || fail "RDS instance left (${rds})"
lgs="$( { aws logs describe-log-groups --region "${REGION}" --log-group-name-prefix "/aws/eks/${CLUSTER}/" \
            --query 'logGroups[].logGroupName' --output text
          aws logs describe-log-groups --region "${REGION}" --log-group-name-prefix "${AGENT_LOGS}" \
            --query 'logGroups[].logGroupName' --output text; } | tr -d '\r' | xargs)"
[ -z "${lgs}" ] && ok "no CloudWatch log groups for ${CLUSTER}" || fail "log groups left: ${lgs}"
if docker info >/dev/null 2>&1; then
  stale="$(stale_project_tags | xargs)"
  [ -z "${stale}" ] && ok "only the newest build of each app is left locally" || fail "older local images left: ${stale}"
fi
state="$(terraform -chdir="${TF_DIR}" state list 2>/dev/null | wc -l)"
[ "${state}" -eq 0 ] && ok "Terraform state empty" || fail "${state} resource(s) still in state"
kc config delete-context "${KUBE_CONTEXT}" >/dev/null 2>&1 || true
# The generated values point at resources that no longer exist; aws-up.sh writes fresh ones.
rm -f "${REPO_ROOT}/helm/values/generated/${ENV_NAME}.yaml" "${REPO_ROOT}"/gitops/operators/*/"generated-${ENV_NAME}.yaml"

echo
if [ "${FAILED}" -eq 0 ] && [ "${DELETING}" -gt 0 ]; then
  log "${ENV_NAME} destroyed; AWS is still deleting ${DELETING} EBS volume(s). Run make aws-down again later to confirm."
elif [ "${FAILED}" -eq 0 ]; then
  log "${ENV_NAME} destroyed; no leftovers found. (The KMS key stays pending deletion for 7 days.)"
else
  log "Teardown incomplete: see FAIL lines above"; exit 1
fi
