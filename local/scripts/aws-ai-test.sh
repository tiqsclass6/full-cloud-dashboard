#!/usr/bin/env bash
# Verify the ops agent on AWS hop by hop without the model: images, internal NLB, Gateway, Runtime, end to end, Hermes.
ENV_NAME="${1:-dev}"
GITOPS_ENV="${ENV_NAME}" KUBE_CONTEXT="lakehouse-${ENV_NAME}"
export GITOPS_ENV KUBE_CONTEXT
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require aws terraform kubectl jq
export MSYS_NO_PATHCONV=1
FAILED=0
TF_DIR="${REPO_ROOT}/terraform/envs/${ENV_NAME}"
tfout() { terraform -chdir="${TF_DIR}" output -raw "$1" | tr -d '\r'; }
REGION="$(tfout region)"; GW="$(tfout agentcore_gateway_id)"; GW_URL="$(tfout agentcore_gateway_url)"; RUNTIME="$(tfout agentcore_runtime_arn)"
PY="${REPO_ROOT}/build/ai-venv/Scripts/python.exe"; [ -x "${PY}" ] || PY="${REPO_ROOT}/build/ai-venv/bin/python"
OUT="${REPO_ROOT}/build/aws-ai-test"; mkdir -p "${OUT}"

log "1. Workloads"
for d in mcp-platform-tools ops-agent ops-control hermes; do
  img="$(kc -n ai-ops get deploy "$d" -o jsonpath='{.spec.template.spec.containers[0].image}')"
  ready="$(kc -n ai-ops get deploy "$d" -o jsonpath='{.status.readyReplicas}')"
  [ "${ready:-0}" -ge 1 ] && [[ "${img}" == *.dkr.ecr.* ]] && ok "$d ready (${img##*/})" || fail "$d not ready (${img})"
done
kc -n ai-ops exec deploy/ops-control -- python -c "
import psycopg, os; c = psycopg.connect(os.environ['DATABASE_URL'], connect_timeout=5)
print(c.execute(\"select count(*) from information_schema.tables where table_schema='ops'\").fetchone()[0], c.info.host)" \
  > "${OUT}/db.txt" 2>&1 && ok "ops-control audit store on RDS: $(awk '{print $1}' "${OUT}/db.txt" | tr -d '\r') tables in schema ops" \
  || { fail "ops-control cannot reach RDS"; cat "${OUT}/db.txt"; }

log "2. Internal NLB"
NLB="$(kc -n ai-ops get svc mcp-platform-tools-vpc -o jsonpath='{.status.loadBalancer.ingress[0].hostname}')"
scheme="$(aws elbv2 describe-load-balancers --region "${REGION}" --query "LoadBalancers[?DNSName=='${NLB}'].Scheme | [0]" --output text | tr -d '\r')"
[ "${scheme}" = internal ] && ok "NLB ${NLB%%.*} is internal" || fail "NLB scheme: ${scheme}"

log "3. AgentCore Gateway"
tid="$(aws bedrock-agentcore-control list-gateway-targets --region "${REGION}" --gateway-identifier "${GW}" --query "items[?name=='platform'].targetId | [0]" --output text | tr -d '\r')"
status="$(aws bedrock-agentcore-control get-gateway-target --region "${REGION}" --gateway-identifier "${GW}" --target-id "${tid}" --query status --output text | tr -d '\r')"
[ "${status}" = READY ] && ok "target platform READY" || fail "target platform: ${status}"
(cd "${REPO_ROOT}/apps/ai-ops" && AWS_REGION="${REGION}" "${PY}" -c "
import asyncio, sys
from ops_agent.backends import McpBackend, SigV4Auth
async def main(url):
    url = url if url.rstrip('/').endswith('/mcp') else url.rstrip('/') + '/mcp'
    async with McpBackend(url, auth=SigV4Auth('${REGION}')) as b:
        tools = await b.list_tools()
        r = await b.call('get_nodes', {})
        print(len(tools), r.is_error, r.text[:200].replace(chr(10), ' '))
asyncio.run(main(sys.argv[1]))" "${GW_URL}") > "${OUT}/gateway.txt" 2>&1
read -r n err rest < "${OUT}/gateway.txt" || true
[ "${n}" = 13 ] && [ "${err}" = False ] && ok "tools/list through the Gateway: 13 tools; get_nodes answered (${rest:0:80}...)" \
  || { fail "Gateway MCP call"; tail -5 "${OUT}/gateway.txt"; }

log "4. AgentCore Runtime"
if [ -z "${RUNTIME}" ]; then
  fail "no runtime (second apply with agentcore_runtime = true)"
else
  aws bedrock-agentcore invoke-agent-runtime --region "${REGION}" --agent-runtime-arn "${RUNTIME}" \
    --runtime-session-id "selftest-$(date +%s)-$(head -c 16 /dev/urandom | od -An -tx1 | tr -d ' \n')" \
    --content-type application/json --accept application/json --payload '{"selftest": true}' \
    --cli-binary-format raw-in-base64-out "${OUT}/selftest.json" > /dev/null 2>"${OUT}/selftest.err" || true
  tools="$(jq -r '.tools | length' "${OUT}/selftest.json" 2>/dev/null)"
  [ "${tools}" = 13 ] && [ "$(jq -r .probe_is_error "${OUT}/selftest.json")" = false ] \
    && ok "runtime selftest: 13 tools, get_kafka_status answered in $(jq -r .seconds "${OUT}/selftest.json") s" \
    || { fail "runtime selftest"; cat "${OUT}/selftest.err" "${OUT}/selftest.json" 2>/dev/null | head -10; }
fi

log "5. End to end: alert -> ops-agent -> AgentCore Runtime -> ops-control (RDS)"
DECISION="$(kc -n ai-ops exec deploy/ops-agent -- python -c "
import json, urllib.request, time
payload = {'groupKey': 'aws-ai-test-%d' % time.time(), 'status': 'firing', 'commonLabels': {'alertname': 'KafkaBrokerDown'},
           'alerts': [{'status': 'firing', 'fingerprint': str(time.time()), 'startsAt': '2026-11-01T00:00:00Z',
                       'labels': {'alertname': 'KafkaBrokerDown', 'severity': 'critical', 'namespace': 'kafka'},
                       'annotations': {'summary': 'test alert from aws-ai-test.sh'}}]}
req = urllib.request.Request('http://localhost:8080/v1/alertmanager', json.dumps(payload).encode(), {'Content-Type': 'application/json'})
print(json.load(urllib.request.urlopen(req))['decision'])" | tr -d '\r')"
[ "${DECISION}" = accepted ] && ok "webhook admitted" || fail "webhook: ${DECISION}"
for _ in $(seq 1 40); do
  kc -n ai-ops logs deploy/ops-agent --since=10m 2>/dev/null | grep -q '"where": "agentcore"' && break; sleep 6
done
line="$(kc -n ai-ops logs deploy/ops-agent --since=10m | grep '"event": "investigation"' | tail -1)"
[ -n "${line}" ] && ok "forwarded to AgentCore Runtime and recorded: $(echo "${line}" | jq -c '{outcome, where, tool_calls}')" \
  || { fail "no investigation from the runtime"; kc -n ai-ops logs deploy/ops-agent --since=10m | tail -5; }
# Only forwarding and recording are tested; outcome "error" means the model call failed (no Bedrock access).
MODEL_ERROR=0
[ "$(echo "${line}" | jq -r '.outcome // empty' 2>/dev/null)" = error ] && MODEL_ERROR=1
kc -n ai-ops exec deploy/ops-control -- python -c "
import psycopg, os; c = psycopg.connect(os.environ['DATABASE_URL'], connect_timeout=5)
r = c.execute(\"select outcome, left(error, 160) from ops.investigations order by started_at desc limit 1\").fetchone(); print(r)" 2>&1 | tail -1 \
  | sed 's/^/        latest in RDS: /'

log "6. Hermes"
kc -n ai-ops exec deploy/hermes -c hermes -- hermes mcp test platform 2>&1 | grep -E "Connected|Tools discovered" | sed 's/^/        /'

echo
if [ "${FAILED}" -ne 0 ]; then
  printf '\033[1;31mOps agent on AWS: checks failed.\033[0m\n'; exit 1
elif [ "${MODEL_ERROR}" -eq 1 ]; then
  printf '\033[1;33mOps agent on AWS: the path works, but the model call failed (outcome=error): no diagnosis was produced.\033[0m\n'
  printf '        See "Not verified" in docs/ai-ops.md (model access in Bedrock).\n'
else
  printf '\033[1;32mOps agent on AWS: all checks passed.\033[0m\n'
fi
