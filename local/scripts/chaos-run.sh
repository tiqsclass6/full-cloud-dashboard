#!/usr/bin/env bash
# Run one chaos experiment, judge alerts, recovery and exactly-once, and write docs/chaos/results/<name>.json.
#   usage: chaos-run.sh <broker-isolation|flink-taskmanager-kill|kafka-latency|zone-outage|ingestion-handoff>
#   CAPTURE=1 also records an ops agent eval fixture after the trigger alert fires
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require kubectl docker jq curl
EXP="${1:?usage: chaos-run.sh <experiment>}"
BASELINE=120 FAULT=360 RATE=100
LOAD=$((BASELINE + FAULT + 180))
RESULTS="${REPO_ROOT}/docs/chaos/results"
# Under the repository (a C:/... path on Windows), so native tools such as jq.exe can open the files.
WORK="${REPO_ROOT}/build/chaos/run-$$"
mkdir -p "${WORK}"
PY="${REPO_ROOT}/.venv/Scripts/python.exe"; [ -x "${PY}" ] || PY="${REPO_ROOT}/.venv/bin/python"
FAILED=0

# Alerts the experiment must trigger, and alerts that must not fire (the platform should absorb it).
case "${EXP}" in
  broker-isolation)       EXPECT="KafkaBrokerDown";  FORBID="IngestErrorBudgetBurnFast IcebergTableStale FlinkJobNotRunning" ;;
  flink-taskmanager-kill) EXPECT="";                 FORBID="IcebergTableStale FlinkCheckpointsStalled FlinkJobNotRunning IngestErrorBudgetBurnFast" ;;
  kafka-latency)          EXPECT="";                 FORBID="IngestErrorBudgetBurnFast FlinkJobNotRunning" ;;
  zone-outage)            EXPECT="KafkaBrokerDown";  FORBID="FlinkJobNotRunning" ;;
  ingestion-handoff)      EXPECT="";                 FORBID="FlinkJobNotRunning IcebergTableStale FlinkCheckpointsStalled" ;;
  *) echo "unknown experiment: ${EXP}" >&2; exit 1 ;;
esac
# Ops agent eval capture (CAPTURE=1): the alert that triggers it, and the alerts included in the payload.
case "${EXP}" in
  broker-isolation)       CAPTURE_ON="KafkaBrokerDown";         CAPTURE_ALERTS="KafkaBrokerDown,KafkaUnderReplicatedPartitions,KafkaNoActiveController,KafkaUnderMinIsr" ;;
  flink-taskmanager-kill) CAPTURE_ON="FlinkCheckpointFailures"; CAPTURE_ALERTS="FlinkCheckpointFailures,FlinkJobRestarting,FlinkCheckpointsStalled" ;;
  zone-outage)            CAPTURE_ON="KafkaBrokerDown";         CAPTURE_ALERTS="KafkaBrokerDown,FlinkJobRestarting,KafkaUnderReplicatedPartitions,KafkaNoActiveController,AlertmanagerClusterDown" ;;
  *)                      CAPTURE_ON="";                        CAPTURE_ALERTS="" ;;
esac
CAPTURE_PID=""

# The paused node may be hosting the target, so a dropped forward is restarted until the run ends.
WATCHDOGS=()
keep_forward() {  # keep_forward <namespace> <svc/name|pod/name> <local:remote>
  local name="${2##*/}"
  ( while [ ! -f "${WORK}/stop" ]; do
      kubectl --context "${KUBE_CONTEXT}" -n "$1" port-forward "$2" "$3" >/dev/null 2>&1 &
      echo $! > "${WORK}/fwd-${name}.pid"
      wait $! || true
      sleep 2
    done ) &
  WATCHDOGS+=($!)
}
PAUSED_NODE="" STREAM="" NOTE=""
cleanup() {
  set +e    # every step must run, even when an earlier one fails (e.g. a node already unpaused)
  [ -n "${STREAM}" ] && kill "${STREAM}" 2>/dev/null
  [ -n "${PAUSED_NODE}" ] && docker unpause "${PAUSED_NODE}" >/dev/null 2>&1
  [ -f "${REPO_ROOT}/chaos/experiments/${EXP}.yaml" ] && \
    kc delete -f "${REPO_ROOT}/chaos/experiments/${EXP}.yaml" --ignore-not-found >/dev/null 2>&1
  touch "${WORK}/stop"
  for f in "${WORK}"/fwd-*.pid; do [ -f "${f}" ] && kill "$(cat "${f}")" 2>/dev/null; done
  for pid in "${WATCHDOGS[@]}"; do kill "${pid}" 2>/dev/null; done
  stop_forwards
  rm -rf "${WORK}"
}
trap cleanup EXIT

prom() { curl -s --max-time 10 -G --data-urlencode "query=$1" http://localhost:9090/api/v1/query | jq -r '.data.result[0].value[1] // empty' 2>/dev/null; }
# Watchdog is kube-prometheus-stack's always-firing heartbeat, not a signal.
firing() { curl -s --max-time 10 http://localhost:9090/api/v1/alerts | jq -r '.data.alerts[]? | select(.state=="firing" and .labels.alertname != "Watchdog") | .labels.alertname' 2>/dev/null | sort -u; }
job_state() { kc -n apps get flinkdeployment events-to-iceberg -o jsonpath='{.status.jobStatus.state}' 2>/dev/null; }
# A partition the group has never consumed reports lag -1; count only real lag.
lag() { prom 'sum(kafka_consumergroup_lag{consumergroup="flink-events-to-iceberg"} >= 0)'; }

log "Preflight (${EXP})"
kc -n chaos-mesh rollout status deploy/chaos-controller-manager --timeout=60s >/dev/null && ok "Chaos Mesh ready" || { fail "Chaos Mesh not ready"; exit 1; }
TARGET_NODE=""
if [ "${EXP}" = zone-outage ]; then
  # A broker's worker that hosts neither Prometheus nor the agent's tools, so the run can still be observed.
  observers="$(kc -n monitoring get pod prometheus-kps-prometheus-0 -o jsonpath='{.spec.nodeName}') $(kc -n ai-ops get pods \
    -l 'app in (mcp-platform-tools,ops-agent)' -o jsonpath='{range .items[*]}{.spec.nodeName}{" "}{end}' 2>/dev/null)"
  for n in $(kc -n kafka get pods -l strimzi.io/broker-role=true -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}'); do
    [[ " ${observers} " != *" ${n} "* ]] && TARGET_NODE="${n}" && break
  done
  [ -n "${TARGET_NODE}" ] || { fail "every broker node hosts an observer (Prometheus or the ops agent tools)"; exit 1; }
  # Losing the JobManager's node stops the job, so FlinkJobNotRunning is then expected, not forbidden.
  jm_node="$(kc -n apps get pods -l app=events-to-iceberg,component=jobmanager -o jsonpath='{.items[0].spec.nodeName}' 2>/dev/null)"
  if [ "${jm_node}" = "${TARGET_NODE}" ]; then
    FORBID="${FORBID/FlinkJobNotRunning/}"; NOTE="the paused node ran the Flink JobManager"
    echo "  note: ${TARGET_NODE} runs the Flink JobManager; FlinkJobNotRunning is expected"
  fi
fi
keep_forward monitoring svc/kps-prometheus 9090:9090
if [ "${EXP}" = ingestion-handoff ]; then
  # The pods are the target: forward through the Service, so each restart of the forward lands on a live pod.
  keep_forward apps svc/ingestion-service 8082:8080
else
  # A port-forward stays bound to one pod: pick an ingestion pod off the node being paused.
  INGEST_POD="$(kc -n apps get pods -l app=ingestion-service --field-selector=status.phase=Running \
    -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.spec.nodeName}{"\n"}{end}' | awk -v n="${TARGET_NODE}" '$2 != n {print $1; exit}')"
  keep_forward apps "pod/${INGEST_POD}" 8082:8080
fi
sleep 6
[ "$(job_state)" = RUNNING ] && ok "Flink job RUNNING" || { fail "Flink job not RUNNING"; exit 1; }
already="$(firing | grep -vxE 'Watchdog|PrometheusMissingRuleEvaluations' | tr '\n' ' ' || true)"
[ -z "${already}" ] && ok "no alerts firing" || echo "  note: already firing before the experiment: ${already}"

# Copies of an event_id that Flink dropped (the counter resets if the job restarts).
dups() { prom 'sum(flink_taskmanager_job_task_operator_duplicatesDropped{job_name="events_to_iceberg"})'; }
DUPS_BEFORE="$(dups)"; DROP_ARGS=()
# The handoff drill also throws away the reply of every 20th batch: the events are stored, the client never hears.
[ "${EXP}" = ingestion-handoff ] && DROP_ARGS=(--drop-reply-every 20)
START_ID="$(date +%s)000"
log "Load: ${RATE} events/s for ${LOAD}s (ids from ${START_ID})"
"${PY}" -I "${LOCAL_DIR}/tools/eo_test.py" stream --start "${START_ID}" --rate "${RATE}" --seconds "${LOAD}" "${DROP_ARGS[@]}" \
  > "${WORK}/stream.json" 2>"${WORK}/stream.err" &
STREAM=$!
sleep "${BASELINE}"

log "Fault: ${EXP} for up to ${FAULT}s"
FAULT_START=$(date +%s)
if [ "${EXP}" = zone-outage ]; then
  PAUSED_NODE="${TARGET_NODE}"
  docker pause "${PAUSED_NODE}" >/dev/null && ok "paused node ${PAUSED_NODE} ($(kc get node "${PAUSED_NODE}" -o jsonpath='{.metadata.labels.topology\.kubernetes\.io/zone}' 2>/dev/null || echo zone))"
else
  kc apply -f "${REPO_ROOT}/chaos/experiments/${EXP}.yaml" >/dev/null && ok "applied chaos/experiments/${EXP}.yaml"
fi

declare -A SEEN=()
MAX_LAG=0 MAX_CKPT=0
while [ $(( $(date +%s) - FAULT_START )) -lt "${FAULT}" ]; do
  t=$(( $(date +%s) - FAULT_START ))
  # Docker Desktop has been seen resuming a paused node on its own; record it instead of assuming.
  if [ -n "${PAUSED_NODE}" ] && [ "$(docker inspect -f '{{.State.Paused}}' "${PAUSED_NODE}" 2>/dev/null)" != true ]; then
    NOTE="${NOTE:+${NOTE}; }node ${PAUSED_NODE} was resumed outside the harness after ${t}s"
    echo "  note: ${NOTE}"; PAUSED_NODE=""; break
  fi
  for a in $(firing); do [ -z "${SEEN[$a]:-}" ] && SEEN[$a]=$t && echo "  +${t}s firing: ${a}"; done
  # Capture 45 s after the trigger alert, so alerts firing close together are in the payload too.
  if [ "${CAPTURE:-0}" = 1 ] && [ -z "${CAPTURE_PID}" ] && [ -n "${CAPTURE_ON}" ] && [ -n "${SEEN[${CAPTURE_ON}]:-}" ] \
     && [ $(( t - SEEN[${CAPTURE_ON}] )) -ge 45 ]; then
    bash "${LOCAL_DIR}/scripts/ai-capture.sh" "${EXP}" "${CAPTURE_ALERTS}" > "${REPO_ROOT}/build/capture-${EXP}.log" 2>&1 &
    CAPTURE_PID=$!; echo "  +${t}s capturing the ops agent eval fixture (build/capture-${EXP}.log)"
  fi
  l="$(lag)"; [ -n "${l}" ] && [ "${l%.*}" -gt "${MAX_LAG}" ] && MAX_LAG="${l%.*}"
  c="$(prom 'max(flink_jobmanager_job_lastCheckpointDuration{job_name="events_to_iceberg"})')"
  [ -n "${c}" ] && [ "${c%.*}" -gt "${MAX_CKPT}" ] && MAX_CKPT="${c%.*}"
  sleep 15
done

log "Fault removed; waiting for recovery"
if [ -n "${PAUSED_NODE}" ]; then docker unpause "${PAUSED_NODE}" >/dev/null 2>&1 || true; PAUSED_NODE=""
else kc delete -f "${REPO_ROOT}/chaos/experiments/${EXP}.yaml" --ignore-not-found >/dev/null; fi
FAULT_END=$(date +%s)
RECOVERED=""
for _ in $(seq 1 90); do   # up to 15 minutes
  t=$(( $(date +%s) - FAULT_START ))
  for a in $(firing); do [ -z "${SEEN[$a]:-}" ] && SEEN[$a]=$t && echo "  +${t}s firing: ${a}"; done
  l="$(lag)"
  if [ "$(job_state)" = RUNNING ] && [ "${l%.*}" = 0 ] && ! kill -0 "${STREAM}" 2>/dev/null; then
    RECOVERED=$(( $(date +%s) - FAULT_END )); break
  fi
  sleep 10
done
STREAM_RC=0; wait "${STREAM}" 2>/dev/null || STREAM_RC=$?
if [ -n "${CAPTURE_PID}" ]; then
  wait "${CAPTURE_PID}" && ok "ops agent fixture captured: evals/agent-scenarios/fixtures/${EXP}.json" \
    || fail "fixture capture failed (build/capture-${EXP}.log)"
fi
[ -n "${RECOVERED}" ] && ok "recovered ${RECOVERED}s after the fault ended (job RUNNING, lag 0)" || fail "no recovery within 15 minutes"
# Alerts raised by the fault should resolve once it is gone (they need their own evaluation time).
for _ in $(seq 1 30); do
  still="$(firing | grep -xE "$(echo "${EXPECT} ${FORBID}" | tr ' ' '|' | sed 's/^|//;s/|$//;s/^$/__none__/')" | tr '\n' ' ' || true)"
  [ -z "${still}" ] && break; sleep 20
done
[ -z "${still}" ] && ok "experiment alerts resolved" || fail "still firing after recovery: ${still}"

log "Data: every event exactly once"
SENT="$(jq -r '.sent // 0' < "${WORK}/stream.json" 2>/dev/null || echo 0)"; RETRIED="$(jq -r '.retried // 0' < "${WORK}/stream.json" 2>/dev/null || echo 0)"
SENT="${SENT:-0}"; RETRIED="${RETRIED:-0}"
DROPPED="$(jq -r '.dropped_replies // 0' < "${WORK}/stream.json" 2>/dev/null || echo 0)"; DROPPED="${DROPPED:-0}"
[ "${STREAM_RC}" = 0 ] && ok "load complete: ${SENT} events sent (${RETRIED} retried batches)" || fail "load generator gave up: $(cat "${WORK}/stream.json" "${WORK}/stream.err" 2>/dev/null | head -2)"
sleep 30   # the last events commit with the next checkpoints
fwd data svc/iceberg-rest 8181:8181; fwd data svc/s3 8333:8333; sleep 5
VERIFY="$("${PY}" -I "${LOCAL_DIR}/tools/eo_test.py" verify-range --start "${START_ID}" --count "${SENT}" 2>&1 | tr -d '\r' | tail -1 || true)"
[ "${SENT}" -gt 0 ] || fail "no events were counted as sent: nothing to verify"
echo "${VERIFY}" | jq -e '.missing == 0 and .duplicates == 0' >/dev/null 2>&1 && ok "Iceberg: ${VERIFY}" || fail "Iceberg: ${VERIFY}"
DUPS_AFTER="$(dups)"; DUPS_DROPPED="null"
[ -n "${DUPS_BEFORE}" ] && [ -n "${DUPS_AFTER}" ] && DUPS_DROPPED=$(( ${DUPS_AFTER%.*} - ${DUPS_BEFORE%.*} ))
# Pods that existed during the run, minus the two replicas: how many the fault replaced.
REPLACED="$(prom "count(count_over_time(kube_pod_info{namespace=\"apps\",pod=~\"ingestion-service-.*\"}[$(( $(date +%s) - FAULT_START + 60 ))s])) - 2")"
REPLACED="${REPLACED%.*}"; REPLACED="${REPLACED:-null}"
if [ "${EXP}" = ingestion-handoff ]; then
  echo "  ${DROPPED} replies discarded, ${REPLACED} ingestion pods replaced, ${DUPS_DROPPED} duplicate copies dropped by Flink"
  # Zero would mean no retry ever produced a second copy in Kafka: the drill would not have tested the lost-ack case.
  [ "${DUPS_DROPPED}" != null ] && [ "${DUPS_DROPPED}" -gt 0 ] && ok "retried copies reached Kafka and were dropped" \
    || fail "no duplicate copies were dropped (${DUPS_DROPPED}): the lost-acknowledgement case was not exercised"
fi

log "Expectations"
for a in ${EXPECT}; do [ -n "${SEEN[$a]:-}" ] && ok "${a} fired (+${SEEN[$a]}s)" || fail "${a} did not fire"; done
for a in ${FORBID}; do [ -z "${SEEN[$a]:-}" ] && ok "${a} did not fire" || fail "${a} fired (+${SEEN[$a]}s)"; done

mkdir -p "${RESULTS}"
alerts_json="$(for a in "${!SEEN[@]}"; do echo "{\"alert\":\"${a}\",\"seconds_after_fault_start\":${SEEN[$a]}}"; done | jq -s 'sort_by(.seconds_after_fault_start)')"
jq -n --arg exp "${EXP}" --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --argjson fault "$((FAULT_END - FAULT_START))" \
  --argjson sent "${SENT:-0}" --argjson retried "${RETRIED:-0}" --argjson recovered "${RECOVERED:-null}" \
  --argjson dropped "${DROPPED:-0}" --argjson dups "${DUPS_DROPPED:-null}" --argjson replaced "${REPLACED:-null}" \
  --argjson maxlag "${MAX_LAG}" --argjson maxckpt "${MAX_CKPT}" --argjson alerts "${alerts_json:-[]}" \
  --argjson verify "$(echo "${VERIFY}" | jq -c . 2>/dev/null || echo null)" --arg expect "${EXPECT}" --arg forbid "${FORBID}" \
  --arg note "${NOTE}" --argjson passed "$([ "${FAILED}" -eq 0 ] && echo true || echo false)" \
  '{experiment: $exp, run_at: $at, fault_seconds: $fault, events_sent: $sent, retried_batches: $retried,
    replies_discarded: $dropped, ingestion_pods_replaced: $replaced, duplicates_dropped_by_flink: $dups,
    recovery_seconds: $recovered, max_consumer_lag: $maxlag, max_checkpoint_ms: $maxckpt, alerts_fired: $alerts,
    expected_alerts: ($expect | split(" ") | map(select(. != ""))), forbidden_alerts: ($forbid | split(" ")),
    iceberg: $verify, note: $note, passed: $passed}' > "${RESULTS}/${EXP}.json"
ok "wrote docs/chaos/results/${EXP}.json"

echo
if [ "${FAILED}" -eq 0 ]; then log "Experiment ${EXP}: PASSED"; else log "Experiment ${EXP}: FAILED"; exit 1; fi
