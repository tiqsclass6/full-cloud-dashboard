#!/usr/bin/env bash
# Runbook: remove duplicate event_id rows (suspend Flink, Spark dedupe and compact, resume, verify with PyIceberg).
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require kubectl

VENV="${REPO_ROOT}/.venv"; PY="${VENV}/Scripts/python.exe"; [ -x "${PY}" ] || PY="${VENV}/bin/python"
trap stop_forwards EXIT
state() { kc -n apps get flinkdeployment events-to-iceberg -o jsonpath="$1" 2>/dev/null || true; }

log "1. Suspend the Flink job (savepoint)"
kc -n apps patch flinkdeployment events-to-iceberg --type merge -p '{"spec":{"job":{"state":"suspended"}}}' >/dev/null
for _ in $(seq 1 60); do [ "$(state '{.status.lifecycleState}')" = "SUSPENDED" ] && break; sleep 4; done
echo "lifecycle: $(state '{.status.lifecycleState}')"
[ "$(state '{.status.lifecycleState}')" = "SUSPENDED" ] || { echo "job did not suspend; not touching the table" >&2; exit 1; }

log "2. Spark: dedupe, then compact"
repair_ok=true
bash "${LOCAL_DIR}/scripts/spark-run.sh" events-repair maintenance --steps dedupe,compact,manifests --min-input-files 2 \
  || repair_ok=false

log "3. Resume the Flink job"
kc -n apps patch flinkdeployment events-to-iceberg --type merge -p '{"spec":{"job":{"state":"running"}}}' >/dev/null
for _ in $(seq 1 80); do [ "$(state '{.status.jobStatus.state}')" = "RUNNING" ] && break; sleep 4; done
echo "job: $(state '{.status.jobStatus.state}')"
[ "${repair_ok}" = true ] || { echo "the Spark repair failed (job resumed, table unchanged or partially rewritten)" >&2; exit 1; }

log "4. Verify (PyIceberg)"
fwd data svc/iceberg-rest 8181:8181; fwd data svc/s3 8333:8333; sleep 4
"${PY}" "${LOCAL_DIR}/tools/table_check.py" && ok "no duplicate event ids" || { fail "duplicates remain"; exit 1; }
