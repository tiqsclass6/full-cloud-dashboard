#!/usr/bin/env bash
# Guardrail test for the ops agent on a running cluster (no model): RBAC, tokens, network policy, approvals, webhook.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require kubectl
FAILED=0
NS=ai-ops
PROBE=aiops-probe
trap 'kc -n apps delete pod "${PROBE}" --ignore-not-found --wait=false >/dev/null 2>&1' EXIT

can() {  # can <service account> <verb> <resource> [namespace]
  kc auth can-i "$2" "$3" ${4:+-n "$4"} --as="system:serviceaccount:${NS}:$1" 2>/dev/null | tr -d '\r'
}
expect_can()    { [ "$(can "$@")" = "yes" ] && ok "$1 can $2 $3 ${4:-}" || fail "$1 cannot $2 $3 ${4:-}"; }
expect_cannot() { [ "$(can "$@")" = "no" ]  && ok "$1 cannot $2 $3 ${4:-}" || fail "$1 CAN $2 $3 ${4:-}"; }
control_py() { kc -n "${NS}" exec deploy/ops-control -- python -c "$1"; }
psql_q() { kc -n data exec statefulset/postgres -- psql -U platform -d platform -tAc "$1" | tr -d '\r'; }

log "1. RBAC"
expect_can    mcp-platform-tools get pods kafka
expect_can    mcp-platform-tools get pods/log apps
expect_can    mcp-platform-tools list nodes
expect_can    mcp-platform-tools list applications.argoproj.io argocd
expect_cannot mcp-platform-tools get secrets kafka
expect_cannot mcp-platform-tools get configmaps apps
expect_cannot mcp-platform-tools delete pods kafka
expect_cannot mcp-platform-tools get pods kube-system
expect_cannot mcp-platform-tools patch flinkdeployments.flink.apache.org apps
expect_cannot ops-agent get pods "${NS}"
expect_cannot ops-agent list nodes
expect_can    ops-control delete pods kafka
expect_cannot ops-control delete pods kube-system
expect_cannot ops-control delete deployments apps
expect_can    ops-control patch flinkdeployments.flink.apache.org/events-to-iceberg apps
expect_cannot ops-control patch flinkdeployments.flink.apache.org/another-job apps
expect_can    ops-control create sparkapplications.sparkoperator.k8s.io apps
expect_cannot ops-control delete sparkapplications.sparkoperator.k8s.io apps
expect_cannot ops-control get secrets "${NS}"

log "2. ops-agent holds no Kubernetes token"
if kc -n "${NS}" exec deploy/ops-agent -- python -c \
    "import os,sys; sys.exit(os.path.exists('/var/run/secrets/kubernetes.io/serviceaccount/token'))"; then
  ok "no service account token mounted in ops-agent"
else
  fail "ops-agent has a service account token"
fi

log "3. Token authentication"
code() { in_cluster_curl -o /dev/null -w '%{http_code}' "$@" 2>/dev/null | tr -d '\r'; }
[ "$(code -X POST -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
     -d '{}' http://mcp-platform-tools.${NS}.svc:8080/mcp)" = "401" ] \
  && ok "MCP endpoint without a token: 401" || fail "MCP endpoint answered without a token"
[ "$(code -X POST -H 'Authorization: Bearer wrong' -d '{}' http://ops-control.${NS}.svc:8080/v1/investigations)" = "401" ] \
  && ok "ops-control with a wrong token: 401" || fail "ops-control accepted a wrong token"
[ "$(code http://ops-control.${NS}.svc:8080/v1/proposals)" = "401" ] \
  && ok "ops-control proposals without a token: 401" || fail "ops-control listed proposals without a token"

log "4. Network policy"
kc apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata: {name: ${PROBE}, namespace: apps, labels: {app: ${PROBE}}}
spec:
  restartPolicy: Never
  securityContext: {runAsNonRoot: true, runAsUser: 1001, seccompProfile: {type: RuntimeDefault}}
  containers:
    - name: probe
      image: ${KAFKA_TOOLS_IMAGE}
      command: ["sleep", "300"]
      resources: {requests: {cpu: 10m, memory: 32Mi}, limits: {memory: 64Mi}}
      securityContext: {allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: {drop: ["ALL"]}}
EOF
kc -n apps wait --for=condition=Ready "pod/${PROBE}" --timeout=120s >/dev/null
for target in ops-control ops-agent mcp-platform-tools; do
  if kc -n apps exec "${PROBE}" -- timeout 4 bash -c "exec 3<>/dev/tcp/${target}.${NS}.svc/8080" >/dev/null 2>&1; then
    fail "apps -> ${target}:8080 is open"
  else
    ok "apps -> ${target}:8080 blocked"
  fi
done

log "5. Approval path (real actions, approved from the CLI)"
# A synthetic investigation, submitted with ops-control's own token from inside its pod.
submit() {  # submit <action> <namespace> <pod> <table>: prints the proposal id
  control_py "
import json, os, time, urllib.request, uuid
inv = {'id': str(uuid.uuid4()), 'started_at': time.time(), 'finished_at': time.time(), 'outcome': 'diagnosed',
       'model': 'test', 'trigger': {'alerts': [{'labels': {'alertname': 'AiOpsTest'}}]}, 'turns': 1,
       'usage': {'input': 0, 'output': 0, 'cache_read': 0, 'cache_write': 0}, 'unsupported_evidence': [],
       'error': '', 'tool_calls': [{'seq': 1, 'tool': 'get_pods', 'arguments': {'namespace': '$2'}, 'is_error': False,
                                    'duration_ms': 1, 'result': 'ai-ops-test'}],
       'diagnosis': {'summary': 'ai-ops-test', 'root_cause_category': 'unknown', 'root_cause': 'test',
                     'confidence': 'low', 'impact': 'none', 'evidence': [{'tool': 'get_pods', 'finding': 'ai-ops-test'}],
                     'proposed_action': {'action': '$1', 'parameters': {'namespace': '$2', 'pod': '$3', 'table': '$4'},
                                         'rationale': 'ai-ops-test'}, 'runbook': '', 'follow_ups': []}}
req = urllib.request.Request('http://localhost:8080/v1/investigations', json.dumps(inv).encode(),
                             {'Authorization': 'Bearer ' + os.environ['CONTROL_TOKEN'], 'Content-Type': 'application/json'})
print(json.load(urllib.request.urlopen(req))['proposal_id'])" | tr -d '\r'
}
hash_of() { psql_q "select params_hash from ops.proposals where id = '$1'"; }
approve() { kc -n "${NS}" exec deploy/ops-control -- python -m ops_agent approve "$@" --approver ai-ops-test >/dev/null 2>&1; }

OLD_POD="$(kc -n apps get pods -l app=iceberg-exporter -o jsonpath='{.items[0].metadata.name}')"
PID="$(submit restart_pod apps "${OLD_POD}" "")"
[ -n "${PID}" ] && [ "${PID}" != "None" ] && ok "proposal stored: restart_pod apps/${OLD_POD}" || fail "no proposal created"
HASH="$(hash_of "${PID}")"
approve "${PID}" --hash 000000000000 && fail "approval with a wrong hash was accepted" || ok "wrong hash refused"
[ "$(psql_q "select status from ops.proposals where id = '${PID}'")" = "pending" ] && ok "still pending" || fail "state changed"
approve "${PID}" --hash "${HASH:0:12}" && ok "approved: action ran" || fail "approval failed"
[ "$(psql_q "select status from ops.proposals where id = '${PID}'")" = "succeeded" ] \
  && ok "proposal succeeded" || fail "proposal is $(psql_q "select status||': '||result from ops.proposals where id = '${PID}'")"
kc -n apps wait --for=delete "pod/${OLD_POD}" --timeout=90s >/dev/null 2>&1 && ok "pod ${OLD_POD} replaced" || fail "pod not restarted"
approve "${PID}" --hash "${HASH:0:12}" && fail "second approval ran again" || ok "second approval refused (single use)"
PID2="$(submit restart_pod kube-system coredns-x "")"
[ "${PID2}" = "None" ] && ok "restart in kube-system rejected at intake" || fail "kube-system proposal stored"
EVENTS="$(psql_q "select string_agg(event, ',' order by id) from ops.audit_log where details->>'proposal' = '${PID}'")"
[ "${EVENTS}" = "proposal_created,approval_denied,approval,approved,execution_succeeded" ] \
  && ok "audit log: ${EVENTS}" || fail "audit log: ${EVENTS}"

log "6. Webhook path"
BEFORE="$(psql_q "select count(*) from ops.investigations where model <> 'test'")"
DECISION="$(kc -n "${NS}" exec deploy/ops-agent -- python -c "
import json, urllib.request, time
payload = {'groupKey': 'ai-ops-test-%d' % time.time(), 'status': 'firing', 'commonLabels': {'alertname': 'AiOpsTest'},
           'alerts': [{'status': 'firing', 'fingerprint': str(time.time()), 'startsAt': '2026-11-01T00:00:00Z',
                       'labels': {'alertname': 'IcebergCompactionOverdue', 'severity': 'warning', 'table': 'lakehouse.events'},
                       'annotations': {'summary': 'test alert from ai-ops-test.sh'}}]}
req = urllib.request.Request('http://localhost:8080/v1/alertmanager', json.dumps(payload).encode(), {'Content-Type': 'application/json'})
print(json.load(urllib.request.urlopen(req))['decision'])" | tr -d '\r')"
[ "${DECISION}" = "accepted" ] && ok "webhook admitted" || fail "webhook: ${DECISION}"
for _ in $(seq 1 60); do
  [ "$(psql_q "select count(*) from ops.investigations where model <> 'test'")" -gt "${BEFORE}" ] && break
  sleep 5
done
OUTCOME="$(psql_q "select outcome || coalesce(' (' || nullif(left(error, 120), '') || ')', '') from ops.investigations where model <> 'test' order by started_at desc limit 1")"
[ "$(psql_q "select count(*) from ops.investigations where model <> 'test'")" -gt "${BEFORE}" ] \
  && ok "investigation recorded: ${OUTCOME}" || fail "no investigation recorded within 5 minutes"

log "7. Slack (clicking the buttons is a manual check: docs/ai-ops.md#slack)"
if kc -n "${NS}" get secret ops-slack >/dev/null 2>&1; then
  kc -n "${NS}" logs deploy/ops-control | grep -q 'slack socket mode connected' \
    && ok "Socket Mode connected" || fail "ops-control has no Socket Mode connection"
  [ "$(psql_q "select slack_ts is not null from ops.proposals where id = '${PID}'")" = "t" ] \
    && ok "test proposal posted to Slack" || fail "test proposal not posted to Slack"
else
  ok "no ops-slack secret: approvals through the CLI only"
fi

echo
[ "${FAILED}" -eq 0 ] && printf '\033[1;32mAll ops agent guardrail checks passed.\033[0m\n' || { printf '\033[1;31mOps agent checks failed.\033[0m\n'; exit 1; }
