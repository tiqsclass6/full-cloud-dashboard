#!/usr/bin/env bash
# End-to-end checks on AWS: Argo CD, Kafka zones, Flink with IRSA, then COUNT events read back exactly by Athena.
#   usage: aws-smoke.sh [env] [events]     (default dev 5000)
ENV_NAME="${1:-dev}"
COUNT="${2:-5000}"
GITOPS_ENV="${ENV_NAME}" KUBE_CONTEXT="lakehouse-${ENV_NAME}"
export GITOPS_ENV KUBE_CONTEXT
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require aws terraform kubectl curl jq python
FAILED=0
trap stop_forwards EXIT

TF_DIR="${REPO_ROOT}/terraform/envs/${ENV_NAME}"
tfout() { terraform -chdir="${TF_DIR}" output -raw "$1"; }
REGION="$(tfout region)"
WORKGROUP="$(tfout athena_workgroup)"
DATABASE="$(tfout glue_database)"

log "Argo CD applications"
while read -r name sync health; do
  [ "${sync}/${health}" = "Synced/Healthy" ] && ok "${name}" || fail "${name}: ${sync}/${health}"
done < <(kc -n argocd get applications -o jsonpath='{range .items[*]}{.metadata.name} {.status.sync.status} {.status.health.status}{"\n"}{end}')

log "Kafka"
zones="$(kc -n kafka get pods -l strimzi.io/broker-role=true -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' \
  | xargs -I{} kubectl --context "${KUBE_CONTEXT}" get node {} -o jsonpath='{.metadata.labels.topology\.kubernetes\.io/zone}{"\n"}' | sort -u | wc -l)"
[ "${zones}" -eq 3 ] && ok "brokers in 3 zones" || fail "brokers in ${zones} zone(s)"
desc="$(kafka_cli kafka-topics.sh --describe --topic events.raw 2>&1)" || true
echo "${desc}" | grep -q "ReplicationFactor: 3" && ok "events.raw RF=3" || fail "events.raw RF != 3"

log "Flink"
for _ in $(seq 1 60); do
  [ "$(kc -n apps get flinkdeployment events-to-iceberg -o jsonpath='{.status.jobStatus.state}' 2>/dev/null)" = "RUNNING" ] && break
  sleep 5
done
state="$(kc -n apps get flinkdeployment events-to-iceberg -o jsonpath='{.status.jobStatus.state}')"
[ "${state}" = "RUNNING" ] && ok "job RUNNING" || fail "job ${state}"
role="$(kc -n apps get pods -l app=events-to-iceberg,component=jobmanager -o jsonpath='{.items[0].spec.containers[0].env[?(@.name=="AWS_ROLE_ARN")].value}')"
[[ "${role}" == *":role/lakehouse-${ENV_NAME}-flink-jobs" ]] && ok "IRSA role injected" || fail "no IRSA role on the JobManager"
fwd apps svc/events-to-iceberg-rest 8081:8081
fwd apps svc/ingestion-service 8082:8080
sleep 5
jid="$(kc -n apps get flinkdeployment events-to-iceberg -o jsonpath='{.status.jobStatus.jobId}')"
# Prints 0 (not an empty string) when the Flink REST port-forward is not up, so the checks below stay numeric.
checkpoints() {
  local n
  n="$(curl -s -m 5 "http://localhost:8081/jobs/${jid}/checkpoints" 2>/dev/null | jq -r '.counts.completed // 0' 2>/dev/null)"
  [[ "${n}" =~ ^[0-9]+$ ]] && echo "${n}" || echo 0
}
for _ in $(seq 1 24); do [ "$(checkpoints)" -gt 0 ] && break; sleep 5; done
[ "$(checkpoints)" -gt 0 ] && ok "$(checkpoints) checkpoints completed (S3)" || fail "no completed checkpoints"

log "Pipeline: ${COUNT} events (+10% duplicate sends) -> Iceberg (Glue) -> Athena"
python -I "${LOCAL_DIR}/tools/eo_test.py" produce --start 0 --count "${COUNT}" --dup-every 10
for _ in $(seq 1 60); do
  lag="$(kafka_cli kafka-consumer-groups.sh \
    --describe --group flink-events-to-iceberg 2>/dev/null | awk 'NR>1 && $6 ~ /^[0-9]+$/ {s+=$6; n++} END {print (n ? s : "unknown")}')"
  [ "${lag}" = "0" ] && break
  sleep 5
done
echo "consumer lag: ${lag}"
sleep 30   # two more checkpoints: the sink commits once per checkpoint

athena() {  # athena <sql>: prints the first result row, comma-separated
  local qid status
  qid="$(aws athena start-query-execution --region "${REGION}" --work-group "${WORKGROUP}" \
    --query-execution-context "Database=${DATABASE}" --query-string "$1" --query QueryExecutionId --output text)"
  for _ in $(seq 1 60); do
    status="$(aws athena get-query-execution --region "${REGION}" --query-execution-id "${qid}" \
      --query QueryExecution.Status.State --output text)"
    case "${status}" in SUCCEEDED|FAILED|CANCELLED) break ;; esac
    sleep 2
  done
  if [ "${status}" != "SUCCEEDED" ]; then
    aws athena get-query-execution --region "${REGION}" --query-execution-id "${qid}" \
      --query QueryExecution.Status.StateChangeReason --output text >&2
    echo "ERR"; return
  fi
  aws athena get-query-results --region "${REGION}" --query-execution-id "${qid}" \
    | jq -r '.ResultSet.Rows[1].Data | map(.VarCharValue) | join(",")'
}
result="$(athena "SELECT count(*), count(DISTINCT event_id) FROM events WHERE type = 'eo.test'")"
echo "Athena: rows,distinct = ${result}"
[ "${result}" = "${COUNT},${COUNT}" ] && ok "Athena: ${COUNT} rows, ${COUNT} distinct (duplicates removed, none lost)" \
  || fail "Athena: expected ${COUNT},${COUNT}, got ${result}"
snapshots="$(athena 'SELECT count(*) FROM "events$snapshots"')"
[ "${snapshots}" != "ERR" ] && [ "${snapshots:-0}" -gt 0 ] && ok "Iceberg metadata via Athena: ${snapshots} snapshots" \
  || fail "Iceberg snapshots query"

echo
if [ "${FAILED}" -eq 0 ]; then log "All AWS smoke checks passed (${ENV_NAME})."; else log "Some checks FAILED"; exit 1; fi
