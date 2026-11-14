#!/usr/bin/env bash
# Schema evolution demo (add an optional region column); needs the job RUNNING on a fresh table (run flink-eo-test.sh first).
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require kubectl curl

REPO_ROOT="$(cd "${LOCAL_DIR}/.." && { pwd -W 2>/dev/null || pwd; })"
TOOL="${LOCAL_DIR}/tools/eo_test.py"
VENV="${REPO_ROOT}/.venv"
PY="${VENV}/Scripts/python.exe"; [ -x "${PY}" ] || PY="${VENV}/bin/python"
[ -x "${PY}" ] || { echo "run flink-eo-test.sh once first (it creates the tool venv)" >&2; exit 1; }

trap stop_forwards EXIT
wait_state() { for _ in $(seq 1 "$3"); do [ "$(kc -n apps get flinkdeployment events-to-iceberg -o "jsonpath=$1" 2>/dev/null || true)" = "$2" ] && return 0; sleep 1; done; echo "timeout waiting for $1=$2" >&2; return 1; }
settle() { sleep 30; }   # lets two checkpoints commit

# Ids 100000+ keep this demo's events distinct from the exactly-once test (ids 0..23999).
log "0. Reset the table and deploy the current ingestion build"
fwd data svc/iceberg-rest 8181:8181; fwd data svc/s3 8333:8333
sleep 5
reset_pipeline
wait_state '{.status.jobStatus.state}' RUNNING 240; sleep 10
bash "${LOCAL_DIR}/scripts/deploy-ingestion.sh" 2>&1 | tail -1

log "1. Run the ingestion service as a schema v1 producer (SCHEMA_RESOURCE=event-v1.avsc)"
# A deliberate, temporary change to a live Deployment: pause GitOps; resuming at the end restores it.
gitops_pause
trap 'gitops_resume; stop_forwards' EXIT
kc -n apps set env deployment/ingestion-service SCHEMA_RESOURCE=/schemas/event-v1.avsc >/dev/null
kc -n apps rollout status deployment/ingestion-service --timeout=300s >/dev/null
fwd apps svc/ingestion-service 8082:8080; ING_PF=${FWD_PID}
sleep 5

log "   500 events with the v1 schema (no region)"
"${PY}" "${TOOL}" produce --start 100000 --count 500
settle

log "2. Add the column to the table (metadata-only)"
JM="$(kc -n apps get pods -l component=jobmanager,app=events-to-iceberg -o name | head -1)"
kc -n apps exec "${JM#pod/}" -- java -cp /opt/flink/usrlib/job.jar io.lakehouse.flink.SchemaTool add-column region string 2>&1 | tail -2
echo "   the job keeps running with its old row layout:"; kc -n apps get flinkdeployment events-to-iceberg

log "3. Restart the job from a savepoint to pick up the new column"
kc -n apps patch flinkdeployment events-to-iceberg --type merge -p '{"spec":{"job":{"state":"suspended"}}}' >/dev/null
wait_state '{.status.lifecycleState}' SUSPENDED 240
kc -n apps patch flinkdeployment events-to-iceberg --type merge -p '{"spec":{"job":{"state":"running"}}}' >/dev/null
wait_state '{.status.jobStatus.state}' RUNNING 240; sleep 10
kc -n apps logs "$(kc -n apps get pods -l component=jobmanager,app=events-to-iceberg -o name | head -1)" 2>/dev/null | grep "Iceberg table" | tail -1 | cut -c1-400

log "4. Switch the ingestion service to schema v2 (it registers the v2 schema with the registry)"
kc -n apps set env deployment/ingestion-service SCHEMA_RESOURCE- >/dev/null
kc -n apps rollout status deployment/ingestion-service --timeout=300s >/dev/null
echo "   registry versions of events.raw-value: $(kc -n data exec deploy/toolbox -- curl -s http://apicurio-registry.data.svc:8080/apis/ccompat/v7/subjects/events.raw-value/versions)"
# The old port-forward is bound to a terminated pod and still holds the port: replace it.
kill "${ING_PF}" 2>/dev/null || true; sleep 2
fwd apps svc/ingestion-service 8082:8080; sleep 3

log "5. 500 events with region=eu-west-1 and 500 without"
"${PY}" "${TOOL}" produce --start 100500 --count 500 --region eu-west-1
"${PY}" "${TOOL}" produce --start 101000 --count 500
settle

log "6. Verify"
if "${PY}" "${TOOL}" verify-evolution --total 1500 --with-region 500 --region eu-west-1; then
  printf '\033[1;32mSCHEMA EVOLUTION DEMO PASSED\033[0m\n'
else
  printf '\033[1;31mSCHEMA EVOLUTION DEMO FAILED\033[0m\n'; exit 1
fi
