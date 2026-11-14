#!/usr/bin/env bash
# DESTRUCTIVE exactly-once test with injected failures (24,000 ids, each once): resets the topic, group and table (local only).
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require kubectl curl

REPO_ROOT="$(cd "${LOCAL_DIR}/.." && { pwd -W 2>/dev/null || pwd; })"
TOOL="${LOCAL_DIR}/tools/eo_test.py"
VENV="${REPO_ROOT}/.venv"
if [ ! -x "${VENV}/Scripts/python.exe" ] && [ ! -x "${VENV}/bin/python" ]; then
  log "Creating tool venv (pyiceberg)"
  uv venv "${VENV}" >/dev/null && uv pip install --python "${VENV}/Scripts/python.exe" "pyiceberg[pyarrow]" >/dev/null 2>&1 \
    || uv pip install --python "${VENV}/bin/python" "pyiceberg[pyarrow]" >/dev/null
fi
PY="${VENV}/Scripts/python.exe"; [ -x "${PY}" ] || PY="${VENV}/bin/python"

trap stop_forwards EXIT

now() { date +%H:%M:%S; }


wait_state() {  # wait_state <jsonpath> <value> <timeout s>
  for _ in $(seq 1 "$3"); do
    [ "$(kc -n apps get flinkdeployment events-to-iceberg -o "jsonpath=$1" 2>/dev/null || true)" = "$2" ] && return 0
    sleep 1
  done
  echo "timed out waiting for $1 = $2" >&2; return 1
}

consumer_lag() {
  kafka_cli kafka-consumer-groups.sh \
    --describe --group flink-events-to-iceberg 2>/dev/null | awk 'NR>1 && $6 ~ /^[0-9]+$/ {s+=$6; n++} END {if (n==0) print "unknown"; else print s}'
}

drain() {  # wait until Flink has consumed everything and two more checkpoints have committed it
  log "[$(now)] waiting for the consumer to catch up"
  for _ in $(seq 1 90); do
    lag="$(consumer_lag)"; [ "${lag}" = "0" ] && break; sleep 4
  done
  echo "lag=${lag}"
  sleep 25
}

log "Port-forwards"
fwd apps svc/ingestion-service 8082:8080
fwd data svc/iceberg-rest 8181:8181
fwd data svc/s3 8333:8333
sleep 5

log "Reset: job, consumer group, topic, table (Argo CD recreates the topic and the job from Git)"
reset_pipeline
wait_state '{.status.jobStatus.state}' RUNNING 240
echo "[$(now)] job RUNNING"
sleep 10

log "Wave 1: baseline"
"${PY}" "${TOOL}" produce --start 0 --count 8000 --dup-every 10
drain
"${PY}" "${TOOL}" verify --expected 8000

log "Wave 2: kill a TaskManager while events are flowing"
"${PY}" "${TOOL}" produce --start 8000 --count 8000 --dup-every 10 &
producer=$!
sleep 3
kc -n apps delete pod -l component=taskmanager,app=events-to-iceberg --wait=false >/dev/null
echo "[$(now)] TaskManager pod deleted"
wait "${producer}"
wait_state '{.status.jobStatus.state}' RUNNING 240
drain
"${PY}" "${TOOL}" verify --expected 16000

log "Wave 3: stop with a savepoint and restore, while events are flowing"
"${PY}" "${TOOL}" produce --start 16000 --count 8000 --dup-every 10 &
producer=$!
sleep 3
kc -n apps patch flinkdeployment events-to-iceberg --type merge -p '{"spec":{"job":{"state":"suspended"}}}' >/dev/null
wait_state '{.status.lifecycleState}' SUSPENDED 240
echo "[$(now)] job suspended; savepoint: $(kc -n apps get flinkdeployment events-to-iceberg -o jsonpath='{.status.jobStatus.upgradeSavepointPath}')"
sleep 5
kc -n apps patch flinkdeployment events-to-iceberg --type merge -p '{"spec":{"job":{"state":"running"}}}' >/dev/null
wait "${producer}"
wait_state '{.status.jobStatus.state}' RUNNING 240
echo "[$(now)] job restored and RUNNING"

log "Re-send 1,000 events delivered before the restart (must be dropped by restored state)"
"${PY}" "${TOOL}" resend --start 0 --count 1000
drain

log "Final verification"
if "${PY}" "${TOOL}" verify --expected 24000; then
  printf '\033[1;32mEXACTLY-ONCE TEST PASSED\033[0m\n'
else
  printf '\033[1;31mEXACTLY-ONCE TEST FAILED\033[0m\n'; exit 1
fi
