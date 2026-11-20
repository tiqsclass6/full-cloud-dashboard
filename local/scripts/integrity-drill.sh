#!/usr/bin/env bash
# Data-integrity drill (local; not destructive to other data): every Kafka offset is accounted for, an undecodable
# record is kept and alerted on, and a silently lost row is found by the offset reconciliation and alerted on.
#   1. 3,000 events through the API (every 10th sent twice), plus 3 records written straight to Kafka that are not Avro
#   2. Flink: the events once each with lineage; the copies and the 3 records in events_skipped; FlinkRecordsUndecodable
#   3. reconcile: nothing missing; then delete one row (the "silent loss"): reconcile names its offset,
#      LakehouseRecordsMissing fires; restore the row: reconcile is clean again
#   env: START (first eo_test index, default from the clock)   RESULT (JSON output, default docs/integrity/results/drill.json)
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require kubectl curl jq
FAILED=0

REPO_ROOT="$(cd "${LOCAL_DIR}/.." && { pwd -W 2>/dev/null || pwd; })"
PY="${REPO_ROOT}/.venv/Scripts/python.exe"; [ -x "${PY}" ] || PY="${REPO_ROOT}/.venv/bin/python"
[ -x "${PY}" ] || { echo "no tool venv: run make local-test-exactly-once once, or create .venv with pyiceberg[pyarrow]" >&2; exit 1; }
# Deleting a row from a partitioned table needs pyiceberg's native transforms.
"${PY}" -c "import pyiceberg_core" 2>/dev/null || uv pip install --python "${PY}" "pyiceberg-core==0.10.1" >/dev/null
EO="${LOCAL_DIR}/tools/eo_test.py"
DRILL="${LOCAL_DIR}/tools/integrity_drill.py"
START="${START:-$(( ($(date +%s) % 100000) * 100 + 20000000 ))}"
COUNT=3000
DUP_EVERY=10
POISON=3
RESULT="${RESULT:-${REPO_ROOT}/docs/integrity/results/drill.json}"
SAVED="${REPO_ROOT}/build/integrity-drill-row.parquet"
mkdir -p "${REPO_ROOT}/build" "$(dirname "${RESULT}")"

trap stop_forwards EXIT
iso() { date -u -d "@$1" +%Y-%m-%dT%H:%M:%SZ; }
prom() { curl -s --max-time 10 -G --data-urlencode "query=$1" http://localhost:9090/api/v1/query | jq -r '.data.result[0].value[1] // empty'; }
alert_state() { curl -s --max-time 10 http://localhost:9090/api/v1/alerts \
  | jq -r --arg a "$1" '[.data.alerts[]? | select(.labels.alertname == $a) | .state] | first // "inactive"'; }
wait_alert() {  # wait_alert <name> <firing|inactive> <timeout s>
  local s
  for _ in $(seq 1 $(($3 / 5))); do s="$(alert_state "$1")"; [ "${s}" = "$2" ] && break; sleep 5; done
  echo "${s}"
}
lag() { kafka_cli kafka-consumer-groups.sh --describe --group flink-events-to-iceberg 2>/dev/null \
  | awk 'NR>1 && $6 ~ /^[0-9]+$/ {s+=$6; n++} END {if (n==0) print "unknown"; else print s}'; }
drain() {
  for _ in $(seq 1 90); do [ "$(lag)" = "0" ] && break; sleep 4; done
  sleep 25   # two more checkpoints commit what was read
}
reconcile() {  # reconcile <name>: one-off run over the drill's window; prints the SUMMARY JSON
  bash "${LOCAL_DIR}/scripts/spark-run.sh" "$1" reconcile --start "$(iso "${T0}")" --end "$(iso $(( $(date +%s) + 60 )))" \
    | grep '^{' | tail -1
}
field() { echo "$1" | jq -r "$2"; }

log "Preconditions"
[ "$(kc -n apps get flinkdeployment events-to-iceberg -o jsonpath='{.status.jobStatus.state}')" = "RUNNING" ] \
  && ok "Flink job running" || { fail "Flink job not running"; exit 1; }
fwd apps svc/ingestion-service 8082:8080
fwd data svc/iceberg-rest 8181:8181
fwd data svc/s3 8333:8333
fwd monitoring svc/kps-prometheus 9090:9090
sleep 5
T0=$(( $(date +%s) - 120 ))
ERR0="$(prom 'sum(flink_taskmanager_job_task_operator_deserializer_deserializationErrors{job_name="events_to_iceberg"})')"
ERR0="${ERR0:-0}"

log "1. ${COUNT} events through the API (every ${DUP_EVERY}th sent twice) and ${POISON} non-Avro records straight to Kafka"
"${PY}" "${EO}" produce --start "${START}" --count "${COUNT}" --dup-every "${DUP_EVERY}"
DUPS=$(( (COUNT + DUP_EVERY - 1) / DUP_EVERY ))
# kafka-admin is the break-glass identity: it stands for a client that bypasses the API's Avro serializer.
for i in $(seq 1 "${POISON}"); do echo "integrity-drill:not avro, record ${i} of drill ${START}"; done \
  | kafka_cli kafka-console-producer.sh --topic events.raw --property parse.key=true --property key.separator=: \
      --producer-property acks=all >/dev/null
ok "sent; waiting for Flink to drain"
drain

log "2. What Flink stored"
RANGE="$("${PY}" "${EO}" verify-range --start "${START}" --count "${COUNT}" || true)"; echo "${RANGE}"
[ "$(field "${RANGE}" .missing)" = "0" ] && [ "$(field "${RANGE}" .duplicates)" = "0" ] \
  && ok "${COUNT} events once each" || fail "events missing or duplicated"
LINEAGE="$("${PY}" "${DRILL}" lineage --start "${START}" --count "${COUNT}")"; echo "${LINEAGE}"
[ "$(field "${LINEAGE}" .with_lineage)" = "${COUNT}" ] && [ "$(field "${LINEAGE}" .distinct_offsets)" = "${COUNT}" ] \
  && ok "every row has its own partition and offset" || fail "rows without lineage"
SKIPPED="$("${PY}" "${DRILL}" skipped --since "$(iso "${T0}")")"; echo "${SKIPPED}" | jq -c '{undecodable, duplicate}'
[ "$(field "${SKIPPED}" .undecodable)" = "${POISON}" ] && ok "${POISON} undecodable records kept with their bytes" \
  || fail "expected ${POISON} undecodable rows"
[ "$(field "${SKIPPED}" .duplicate)" = "${DUPS}" ] && ok "${DUPS} duplicate copies recorded" \
  || fail "expected ${DUPS} duplicate rows"
ERR1="$(prom 'sum(flink_taskmanager_job_task_operator_deserializer_deserializationErrors{job_name="events_to_iceberg"})')"
echo "deserializationErrors ${ERR0} -> ${ERR1}"
UNDECODABLE_ALERT="$(wait_alert FlinkRecordsUndecodable firing 180)"
[ "${UNDECODABLE_ALERT}" = "firing" ] && ok "FlinkRecordsUndecodable firing" || fail "FlinkRecordsUndecodable: ${UNDECODABLE_ALERT}"

log "3a. Reconcile: nothing missing"
CLEAN="$(reconcile integrity-reconcile-1)"; echo "${CLEAN}" | jq -c 'del(.target)'
[ "$(field "${CLEAN}" .missing)" = "0" ] && [ "$(field "${CLEAN}" .duplicated_offsets)" = "0" ] \
  && ok "every offset accounted for" || fail "reconcile not clean before the drill"

log "3b. Silent loss: delete one stored row"
LOST="$("${PY}" "${DRILL}" delete-one --event-index $(( START + 1234 )) --save "${SAVED}")"; echo "${LOST}"
EXPECT_RANGE="p$(field "${LOST}" .partition):$(field "${LOST}" .offset)-$(field "${LOST}" .offset)"
DETECTED="$(reconcile integrity-reconcile-2)"; echo "${DETECTED}" | jq -c 'del(.target)'
[ "$(field "${DETECTED}" .missing)" = "1" ] && [ "$(field "${DETECTED}" '.missing_ranges[0]')" = "${EXPECT_RANGE}" ] \
  && ok "reconcile names the lost record: ${EXPECT_RANGE}" || fail "loss not detected as ${EXPECT_RANGE}"
MISSING_ALERT="$(wait_alert LakehouseRecordsMissing firing 240)"
[ "${MISSING_ALERT}" = "firing" ] && ok "LakehouseRecordsMissing firing" || fail "LakehouseRecordsMissing: ${MISSING_ALERT}"

log "3c. Restore the row"
"${PY}" "${DRILL}" restore --file "${SAVED}"
RESTORED="$(reconcile integrity-reconcile-3)"; echo "${RESTORED}" | jq -c 'del(.target)'
[ "$(field "${RESTORED}" .missing)" = "0" ] && ok "clean again" || fail "still missing after restore"
CLEARED="$(wait_alert LakehouseRecordsMissing inactive 240)"
[ "${CLEARED}" = "inactive" ] && ok "LakehouseRecordsMissing resolved" || fail "LakehouseRecordsMissing: ${CLEARED}"
kc -n apps delete sparkapplication integrity-reconcile-1 integrity-reconcile-2 integrity-reconcile-3 --ignore-not-found >/dev/null

jq -n --arg date "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --argjson start "${START}" --argjson count "${COUNT}" \
  --argjson range "${RANGE}" --argjson lineage "${LINEAGE}" --argjson skipped "$(echo "${SKIPPED}" | jq 'del(.undecodable_records[].payload)')" \
  --arg errors_before "${ERR0}" --arg errors_after "${ERR1}" --arg undecodable_alert "${UNDECODABLE_ALERT}" \
  --argjson clean "${CLEAN}" --argjson lost "$(echo "${LOST}" | jq '.saved = "build/integrity-drill-row.parquet"')" --argjson detected "${DETECTED}" --arg missing_alert "${MISSING_ALERT}" \
  --argjson restored "${RESTORED}" --arg cleared "${CLEARED}" --argjson failed "${FAILED}" \
  '{date: $date, start: $start, events: $count, verify_range: $range, lineage: $lineage, skipped: $skipped,
    deserialization_errors: {before: $errors_before, after: $errors_after}, undecodable_alert: $undecodable_alert,
    reconcile_clean: $clean, deleted_row: $lost, reconcile_after_delete: $detected, missing_alert: $missing_alert,
    reconcile_after_restore: $restored, missing_alert_after_restore: $cleared, passed: ($failed == 0)}' > "${RESULT}"
echo "result: ${RESULT}"
exit "${FAILED}"
