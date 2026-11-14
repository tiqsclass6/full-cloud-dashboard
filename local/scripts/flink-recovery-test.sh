#!/usr/bin/env bash
# Regression test (docs/gitops.md): a FAILED job must recover from the latest checkpoint, not an old savepoint.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require kubectl

VENV="${REPO_ROOT}/.venv"; PY="${VENV}/Scripts/python.exe"; [ -x "${PY}" ] || PY="${VENV}/bin/python"
[ -x "${PY}" ] || { echo "run flink-eo-test.sh once first (it creates the tool venv)" >&2; exit 1; }
trap 'gitops_resume >/dev/null 2>&1; stop_forwards' EXIT
now() { date +%H:%M:%S; }
job() { kc -n apps get flinkdeployment events-to-iceberg -o jsonpath='{.status.jobStatus.state}' 2>/dev/null || true; }
kafka_total() {
  kafka_cli kafka-get-offsets.sh --topic events.raw 2>/dev/null \
    | awk -F: '{s+=$3} END {print s+0}'
}
lag() {
  kafka_cli kafka-consumer-groups.sh --describe \
    --group flink-events-to-iceberg 2>/dev/null | awk 'NR>1 && $6 ~ /^[0-9]+$/ {s+=$6; n++} END {print (n ? s : "unknown")}'
}
drain() { for _ in $(seq 1 120); do [ "$(lag)" = "0" ] && break; sleep 4; done; sleep 25; }
rows() {  # prints "rows distinct" from an independent reader
  "${PY}" "${LOCAL_DIR}/tools/table_check.py" | "${PY}" -c "import sys, json; d = json.load(sys.stdin); print(d['rows'], d['distinct_ids'])"
}
send() { bash "${LOCAL_DIR}/scripts/loadtest.sh" 2000 10 1 50 16 | grep '^RESULT' | cut -c1-200; }

fwd data svc/iceberg-rest 8181:8181; fwd data svc/s3 8333:8333; sleep 4
[ "$(job)" = "RUNNING" ] || { echo "the Flink job must be RUNNING to start (is: $(job))" >&2; exit 1; }
drain
read -r R0 D0 <<< "$(rows)"; K0="$(kafka_total)"
echo "[$(now)] start: table rows=${R0} distinct=${D0}  kafka=${K0}"
[ "${R0}" = "${D0}" ] || { echo "table already has duplicates; run repair-duplicates.sh first" >&2; exit 1; }

log "1. Baseline: 20,000 events"
send; drain
read -r R1 D1 <<< "$(rows)"; K1="$(kafka_total)"
echo "[$(now)] table +$((R1 - R0)) rows, kafka +$((K1 - K0)) messages"

log "2. Fault: catalog down, JobManager killed, 20,000 events arrive meanwhile"
gitops_pause                                   # the catalog scale-down is deliberate drift
stop_forwards                                  # the catalog port-forward dies with its pod
kc -n data scale deployment/iceberg-rest --replicas=0 >/dev/null
kc -n apps delete pod -l component=jobmanager,app=events-to-iceberg --wait=false >/dev/null
echo "[$(now)] catalog scaled to 0, JobManager pod deleted"
send
for _ in $(seq 1 60); do [ "$(job)" = "FAILED" ] && break; sleep 5; done
echo "[$(now)] job state during the outage: $(job)"

log "3. Recovery: catalog back, the operator restarts the failed job by itself"
kc -n data scale deployment/iceberg-rest --replicas=1 >/dev/null
kc -n data rollout status deployment/iceberg-rest --timeout=300s >/dev/null
gitops_resume
for _ in $(seq 1 120); do [ "$(job)" = "RUNNING" ] && break; sleep 5; done
echo "[$(now)] job: $(job)"
JM="$(kc -n apps get pods -l component=jobmanager,app=events-to-iceberg -o name | head -1)"
kc -n apps logs "${JM#pod/}" 2>/dev/null | grep -E "Restoring job .* from Checkpoint|execution.state-recovery.path|execution.savepoint.path" \
  | tail -2 | sed 's/^.*INFO /  /' | cut -c1-260

log "4. Verify"
fwd data svc/iceberg-rest 8181:8181; fwd data svc/s3 8333:8333; sleep 4
drain
read -r R2 D2 <<< "$(rows)"; K2="$(kafka_total)"
echo "[$(now)] since step 1: table +$((R2 - R1)) rows, kafka +$((K2 - K1)) messages; rows=${R2} distinct=${D2}"
if [ "$((R2 - R1))" = "$((K2 - K1))" ] && [ "${R2}" = "${D2}" ] && [ "$((R1 - R0))" = "$((K1 - K0))" ]; then
  printf '\033[1;32mRECOVERY TEST PASSED: no loss, no duplicates\033[0m\n'
else
  printf '\033[1;31mRECOVERY TEST FAILED\033[0m\n'; exit 1
fi
