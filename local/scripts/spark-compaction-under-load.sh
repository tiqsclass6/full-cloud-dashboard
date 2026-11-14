#!/usr/bin/env bash
# Compaction while Flink writes under load: the table must still hold every accepted event exactly once.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require kubectl

RATE="${1:-5000}"; DURATION="${2:-100}"
REPO_ROOT="$(cd "${LOCAL_DIR}/.." && { pwd -W 2>/dev/null || pwd; })"
VENV="${REPO_ROOT}/.venv"; PY="${VENV}/Scripts/python.exe"; [ -x "${PY}" ] || PY="${VENV}/bin/python"
[ -x "${PY}" ] || { echo "run flink-eo-test.sh once first (it creates the tool venv)" >&2; exit 1; }
trap stop_forwards EXIT

kafka_total() {
  kafka_cli kafka-get-offsets.sh --topic events.raw 2>/dev/null | awk -F: '{s+=$3} END {print s+0}'
}
consumer_lag() {
  kafka_cli kafka-consumer-groups.sh --describe --group flink-events-to-iceberg 2>/dev/null \
    | awk 'NR>1 && $6 ~ /^[0-9]+$/ {s+=$6} END {print s+0}'
}

log "Before: Flink state and Kafka total"
kc -n apps get flinkdeployment events-to-iceberg
restarts_before="$(kc -n apps get pods -l app=events-to-iceberg -o jsonpath='{.items[*].status.containerStatuses[*].restartCount}')"
echo "Kafka messages before: $(kafka_total)"

log "Start load: ${RATE} events/s for ${DURATION}s (background)"
bash "${LOCAL_DIR}/scripts/loadtest.sh" "${RATE}" "${DURATION}" 5 50 64 > /tmp/underload-load.log 2>&1 &
load_pid=$!

sleep 25
log "Compaction #1 while Flink is writing"
bash "${LOCAL_DIR}/scripts/spark-run.sh" compact-under-load-1 maintenance --steps compact,manifests \
  --min-input-files 2 | tee /tmp/underload-c1.log | cut -c1-700
sleep 5
log "Compaction #2 while Flink is still writing"
bash "${LOCAL_DIR}/scripts/spark-run.sh" compact-under-load-2 maintenance --steps compact,manifests \
  --min-input-files 2 | tee /tmp/underload-c2.log | cut -c1-700

wait "${load_pid}" || true
grep '^RESULT' /tmp/underload-load.log | cut -c1-400

log "Wait for Flink to drain"
for _ in $(seq 1 60); do [ "$(consumer_lag)" = "0" ] && break; sleep 4; done
sleep 20   # two checkpoints so the last files are committed

log "Verify against Kafka (independent reader: PyIceberg)"
total="$(kafka_total)"
echo "Kafka messages after: ${total}"
fwd data svc/iceberg-rest 8181:8181; fwd data svc/s3 8333:8333; sleep 4
if "${PY}" "${LOCAL_DIR}/tools/table_check.py" --expect-rows "${total}"; then
  verdict=PASSED
else
  verdict=FAILED
fi
echo "Flink: $(kc -n apps get flinkdeployment events-to-iceberg -o jsonpath='{.status.jobStatus.state}')," \
     "pod restarts before/after: [${restarts_before}] / [$(kc -n apps get pods -l app=events-to-iceberg -o jsonpath='{.items[*].status.containerStatuses[*].restartCount}')]"
[ "${verdict}" = "PASSED" ] && printf '\033[1;32mCOMPACTION-UNDER-LOAD TEST PASSED\033[0m\n' \
  || { printf '\033[1;31mCOMPACTION-UNDER-LOAD TEST FAILED\033[0m\n'; exit 1; }
