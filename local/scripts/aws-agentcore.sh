#!/usr/bin/env bash
# Connect AgentCore Gateway to the MCP server in EKS, or remove that connection (docs/ai-ops.md).
#   aws-agentcore.sh [env] up     after aws-up.sh
#   aws-agentcore.sh [env] down   before terraform destroy
ENV_NAME="${1:-dev}"; ACTION="${2:-up}"
GITOPS_ENV="${ENV_NAME}" KUBE_CONTEXT="lakehouse-${ENV_NAME}"
export GITOPS_ENV KUBE_CONTEXT
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require aws terraform kubectl jq openssl
export MSYS_NO_PATHCONV=1

TF_DIR="${REPO_ROOT}/terraform/envs/${ENV_NAME}"
tfout() { terraform -chdir="${TF_DIR}" output -raw "$1" 2>/dev/null | tr -d '\r' || true; }
REGION="$(tfout region)"; GW="$(tfout agentcore_gateway_id)"; CLUSTER="$(tfout cluster_name)"
# After a partly failed `terraform destroy` the state has no outputs: they print nothing and exit 0.
REGION="${REGION:-us-east-1}"; CLUSTER="${CLUSTER:-lakehouse-${ENV_NAME}}"
[ "${ACTION}" = down ] || [ -n "${GW}" ] || { echo "no Terraform outputs for ${ENV_NAME}: is the environment applied?" >&2; exit 1; }
PROVIDER="${CLUSTER}-mcp-token"; TARGET=platform
HOSTNAME_TLS="$(grep -A4 'vpcEndpoint:' "${REPO_ROOT}/helm/charts/ai-ops/values.yaml" | awk '/hostname:/{print $2}')"
ac() { aws bedrock-agentcore-control --region "${REGION}" "$@"; }
target_id() { ac list-gateway-targets --gateway-identifier "${GW}" --query "items[?name=='${TARGET}'].targetId | [0]" --output text | tr -d '\r'; }

WORK="${REPO_ROOT}/build/agentcore-${ENV_NAME}"
mkdir -p "${WORK}"; chmod 700 "${WORK}"
trap 'rm -f "${WORK}"/*.key "${WORK}"/*.json' EXIT

if [ "${ACTION}" = down ]; then
  log "Removing the Gateway target and the API key provider"
  tid=""; [ -n "${GW}" ] && tid="$(target_id)"      # no Gateway id: it is already gone from the state
  if [ -n "${tid}" ] && [ "${tid}" != None ]; then
    ac delete-gateway-target --gateway-identifier "${GW}" --target-id "${tid}" >/dev/null
    for _ in $(seq 1 60); do [ "$(target_id)" = None ] && break; sleep 5; done
    ok "target ${TARGET} deleted"
  else ok "no target"; fi
  ac delete-api-key-credential-provider --name "${PROVIDER}" >/dev/null 2>&1 && ok "provider ${PROVIDER} deleted" || ok "no provider"
  exit 0
fi

log "0. Shared secrets and TLS (aws-ai-secrets.sh; aws-up.sh already ran it, so these are no-ops)"
bash "${LOCAL_DIR}/scripts/aws-ai-secrets.sh" "${ENV_NAME}"

log "2. Internal NLB"
NLB=""
for _ in $(seq 1 60); do
  NLB="$(kc -n ai-ops get svc mcp-platform-tools-vpc -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null)"
  [ -n "${NLB}" ] && break; sleep 10
done
[ -n "${NLB}" ] || { fail "no load balancer for ai-ops/mcp-platform-tools-vpc"; exit 1; }
ok "${NLB}"

log "3. API key provider ${PROVIDER}"
TOKEN="$(kc -n ai-ops get secret ai-ops-internal -o jsonpath='{.data.MCP_TOKEN}' | base64 -d)"
jq -n --arg name "${PROVIDER}" --arg key "${TOKEN}" '{name: $name, apiKey: $key}' > "${WORK}/provider.json"
unset TOKEN
if ac get-api-key-credential-provider --name "${PROVIDER}" >/dev/null 2>&1; then
  ac update-api-key-credential-provider --cli-input-json "file://${WORK}/provider.json" >/dev/null
else
  ac create-api-key-credential-provider --cli-input-json "file://${WORK}/provider.json" >/dev/null
fi
PROVIDER_ARN="$(ac get-api-key-credential-provider --name "${PROVIDER}" --query credentialProviderArn --output text | tr -d '\r')"
ok "${PROVIDER_ARN##*/}"

log "4. Gateway target ${TARGET}"
tid="$(target_id)"
if [ -n "${tid}" ] && [ "${tid}" != None ]; then
  ac delete-gateway-target --gateway-identifier "${GW}" --target-id "${tid}" >/dev/null
  for _ in $(seq 1 60); do [ "$(target_id)" = None ] && break; sleep 5; done
fi
jq -n --arg gw "${GW}" --arg name "${TARGET}" --arg url "https://${HOSTNAME_TLS}/mcp" --arg provider "${PROVIDER_ARN}" \
      --arg vpc "$(tfout vpc_id)" --arg subnets "$(tfout private_subnets)" --arg sg "$(tfout agentcore_gateway_egress_sg)" \
      --arg nlb "${NLB}" --arg ca "$(tfout mcp_ca_secret_arn)" '{
  gatewayIdentifier: $gw, name: $name,
  description: "The platform read-only MCP tools (mcp-platform-tools in EKS, internal NLB)",
  targetConfiguration: {mcp: {mcpServer: {endpoint: $url}}},
  # The Gateway puts a space between the prefix and the key itself ("Bearer" + " " + key).
  credentialProviderConfigurations: [{credentialProviderType: "API_KEY", credentialProvider: {apiKeyCredentialProvider: {
    providerArn: $provider, credentialParameterName: "Authorization", credentialPrefix: "Bearer", credentialLocation: "HEADER"}}}],
  privateEndpoint: {managedVpcResource: {vpcIdentifier: $vpc, subnetIds: ($subnets | split(",")),
    endpointIpAddressType: "IPV4", securityGroupIds: [$sg], routingDomain: $nlb}},
  certificateConfigurations: [{secretsManager: {secretArn: $ca}}]}' > "${WORK}/target.json"
ac create-gateway-target --cli-input-json "file://${WORK}/target.json" >/dev/null
for _ in $(seq 1 90); do
  status="$(ac get-gateway-target --gateway-identifier "${GW}" --target-id "$(target_id)" --query status --output text | tr -d '\r')"
  case "${status}" in READY|FAILED|*_FAILED) break ;; esac
  sleep 10
done
if [ "${status}" = READY ]; then
  ok "target READY: AgentCore Gateway reached the MCP server inside the VPC and listed its tools"
else
  fail "target ${status}"
  ac get-gateway-target --gateway-identifier "${GW}" --target-id "$(target_id)" --query statusReasons --output text
  exit 1
fi
