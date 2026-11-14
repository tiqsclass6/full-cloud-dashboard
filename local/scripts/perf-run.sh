#!/usr/bin/env bash
# Controlled performance run (one replica, chosen GC, JFR); the recording is copied out and summarised.
#   usage: perf-run.sh <label> <g1|zgc> [events/s] [duration s] [batch]
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require kubectl python

LABEL="${1:?label required}"; GC="${2:?gc required: g1|zgc}"
RATE="${3:-30000}"; DURATION="${4:-70}"; BATCH="${5:-50}"

case "$GC" in
  g1)  GC_FLAGS="-XX:+UseG1GC" ;;
  zgc) GC_FLAGS="-XX:+UseZGC -XX:+ZGenerational" ;;   # generational ZGC is opt-in on JDK 21
  *) echo "unknown gc: $GC" >&2; exit 1 ;;
esac

REPO_ROOT="$(cd "${LOCAL_DIR}/.." && { pwd -W 2>/dev/null || pwd; })"
OUT_DATA="docs/performance/data"; OUT_RESULTS="docs/performance/results"
mkdir -p "${REPO_ROOT}/${OUT_DATA}" "${REPO_ROOT}/${OUT_RESULTS}"

log "Configuring one replica: ${GC} (${GC_FLAGS}), JFR profile settings"
# Deliberate live changes: pause GitOps; resuming (also on failure) restores the deployment from Git.
gitops_pause
trap gitops_resume EXIT
kc -n apps scale deployment/ingestion-service --replicas=1 >/dev/null
kc -n apps set env deployment/ingestion-service \
  JAVA_OPTS="-XX:MaxRAMPercentage=70 -XX:+ExitOnOutOfMemoryError ${GC_FLAGS} -XX:StartFlightRecording=name=perf,settings=profile,disk=true,filename=/tmp/${LABEL}.jfr,maxsize=512m" >/dev/null
kc -n apps rollout status deployment/ingestion-service --timeout=300s >/dev/null
# Old-ReplicaSet pods linger in Terminating after the rollout; wait until exactly one live pod is left.
for _ in $(seq 1 60); do
  [ "$(kc -n apps get pods -l app=ingestion-service --no-headers | grep -vc Terminating)" -eq 1 ] && break
  sleep 2
done
POD="$(kc -n apps get pods -l app=ingestion-service --sort-by=.metadata.creationTimestamp -o name | tail -1)"
POD="${POD#pod/}"
echo "pod: ${POD}"
sleep 15   # let the JVM settle before measuring

log "Load test: ${RATE} events/s, batch ${BATCH}, ${DURATION}s"
RESULT="$(bash "${LOCAL_DIR}/scripts/loadtest.sh" "${RATE}" "${DURATION}" 15 "${BATCH}" 64 | grep '^RESULT ' | sed 's/^RESULT //')"
echo "${RESULT}"

log "Extracting the flight recording"
kc -n apps exec "${POD}" -- jcmd 1 JFR.dump name=perf "filename=/tmp/${LABEL}.jfr" >/dev/null
# `kubectl cp` mis-parses drive letters (C:), so run it from the repo root with a relative destination.
(cd "${REPO_ROOT}" && kc cp "apps/${POD}:/tmp/${LABEL}.jfr" "${OUT_DATA}/${LABEL}.jfr")

log "Summarising"
python -I "${LOCAL_DIR}/tools/jfr_summary.py" "${REPO_ROOT}/${OUT_DATA}/${LABEL}.jfr" \
  --label "${LABEL}" --gc "${GC}" --load "${RESULT}" > "${REPO_ROOT}/${OUT_RESULTS}/${LABEL}.json"
cat "${REPO_ROOT}/${OUT_RESULTS}/${LABEL}.json"

log "Restoring the deployment from Git"
gitops_resume
trap - EXIT
sleep 10
kc -n apps rollout status deployment/ingestion-service --timeout=300s >/dev/null
