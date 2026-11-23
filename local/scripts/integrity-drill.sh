#!/usr/bin/env bash
# Data-integrity drill (local; not destructive to other data): every Kafka offset is accounted for, an undecodable
# record is kept and alerted on, and a silently lost row is found by the offset reconciliation and alerted on.
#   1. 3,000 events through the API (every 10th sent twice), plus 3 records written straight to Kafka that are not Avro
#   2. Flink: the events once each with lineage; the copies and the 3 records in events_skipped; FlinkRecordsUndecodable
#   3. reconcile: nothing missing; then delete one row (the "silent loss"): reconcile names its offset,
#      LakehouseRecordsMissing fires; restore the row: reconcile is clean again
#   4. replay tool: the 3 records are still undecodable (nothing sent); a deleted row older than Flink's
#      de-duplication window is found, sent again through the API, stored once, and reconcile is clean
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

log "4a. Replay the undecodable records (dry run): they are still not Avro, so nothing is sent"
REPLAY_UNDECODABLE="$(bash "${LOCAL_DIR}/scripts/replay.sh" undecodable --since "$(iso "${T0}")" | grep '^SUMMARY ' | sed 's/^SUMMARY //')"
echo "${REPLAY_UNDECODABLE}"
[ "$(field "${REPLAY_UNDECODABLE}" '.outcomes.UNDECODABLE')" = "${POISON}" ] && [ "$(field "${REPLAY_UNDECODABLE}" .to_replay)" = "0" ] \
  && ok "${POISON} reported as still undecodable, none sent" || fail "unexpected undecodable replay plan"

log "4b. Replay a lost record: delete a row older than the de-duplication window, find it, send it again"
OLD="$("${PY}" "${DRILL}" old-row --older-than-minutes 75)"; echo "${OLD}"
REPLAY_MISSING='{"skipped": "no stored row older than 75 minutes"}'
if [ "$(field "${OLD}" .found)" = "true" ]; then
  P="$(field "${OLD}" .partition)"; O="$(field "${OLD}" .offset)"; ID="$(field "${OLD}" .event_id)"
  OLD_TS="$(date -u -d "$(field "${OLD}" .kafka_timestamp)" +%s)"
  "${PY}" "${DRILL}" delete-one --event-id "${ID}" --save "${REPO_ROOT}/build/integrity-replay-row.parquet" >/dev/null
  window() { bash "${LOCAL_DIR}/scripts/spark-run.sh" "$1" reconcile --start "$(iso $((OLD_TS - 300)))" --end "$(iso $((OLD_TS + 300)))" | grep '^{' | tail -1; }
  LOST_OLD="$(window integrity-reconcile-4)"; echo "${LOST_OLD}" | jq -c '{checked_offsets, missing, missing_ranges}'
  [ "$(field "${LOST_OLD}" '.missing_ranges[0]')" = "p${P}:${O}-${O}" ] && ok "reconcile names p${P}:${O}" || fail "loss of p${P}:${O} not detected"
  PLAN="$(bash "${LOCAL_DIR}/scripts/replay.sh" missing --partition "${P}" --offset "${O}" | grep '^{' | jq -r .outcome)"
  [ "${PLAN}" = "REPLAY" ] && ok "dry run plans the replay" || fail "dry run: ${PLAN}"
  APPLIED="$(bash "${LOCAL_DIR}/scripts/replay.sh" missing --partition "${P}" --offset "${O}" --apply | grep '^SUMMARY ' | sed 's/^SUMMARY //')"
  echo "${APPLIED}"
  [ "$(field "${APPLIED}" .sent)" = "1" ] && ok "sent through the ingestion API" || fail "replay not sent"
  drain
  EVENT="$("${PY}" "${DRILL}" event --event-id "${ID}")"; echo "${EVENT}"
  [ "$(field "${EVENT}" .rows)" = "1" ] && [ "$(field "${EVENT}" '.copies[0].replayed_from' | cut -d: -f1-2)" = "${P}:${O}" ] \
    && ok "the event is back once, marked as the replay of p${P}:${O}" || fail "replayed event not stored once"
  AGAIN="$(bash "${LOCAL_DIR}/scripts/replay.sh" missing --partition "${P}" --offset "${O}" | grep '^{' | jq -r .outcome)"
  [ "${AGAIN}" = "ALREADY_REPLAYED" ] && ok "a second replay is refused" || fail "second replay: ${AGAIN}"
  HEALED="$(window integrity-reconcile-5)"; echo "${HEALED}" | jq -c '{checked_offsets, missing, replayed, duplicated_offsets}'
  [ "$(field "${HEALED}" .missing)" = "0" ] && [ "$(field "${HEALED}" .replayed)" = "1" ] \
    && ok "reconcile clean: the replay accounts for p${P}:${O}" || fail "reconcile after replay not clean"
  REPLAY_MISSING="$(jq -n --argjson old "${OLD}" --argjson lost "${LOST_OLD}" --arg plan "${PLAN}" --argjson applied "${APPLIED}" \
    --argjson event "${EVENT}" --arg again "${AGAIN}" --argjson healed "${HEALED}" \
    '{row: $old, reconcile_after_delete: $lost, dry_run: $plan, applied: $applied, event_after: $event,
      second_dry_run: $again, reconcile_after_replay: $healed}')"
  kc -n apps delete sparkapplication integrity-reconcile-4 integrity-reconcile-5 --ignore-not-found >/dev/null
else
  echo "  skipped: no stored row older than 75 minutes (run the drill again in an hour)"
fi

jq -n --arg date "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --argjson start "${START}" --argjson count "${COUNT}" \
  --argjson range "${RANGE}" --argjson lineage "${LINEAGE}" --argjson skipped "$(echo "${SKIPPED}" | jq 'del(.undecodable_records[].payload)')" \
  --arg errors_before "${ERR0}" --arg errors_after "${ERR1}" --arg undecodable_alert "${UNDECODABLE_ALERT}" \
  --argjson clean "${CLEAN}" --argjson lost "$(echo "${LOST}" | jq '.saved = "build/integrity-drill-row.parquet"')" --argjson detected "${DETECTED}" --arg missing_alert "${MISSING_ALERT}" \
  --argjson restored "${RESTORED}" --arg cleared "${CLEARED}" --argjson failed "${FAILED}" \
  --argjson replay_undecodable "${REPLAY_UNDECODABLE}" --argjson replay_missing "${REPLAY_MISSING}" \
  '{date: $date, start: $start, events: $count, verify_range: $range, lineage: $lineage, skipped: $skipped,
    deserialization_errors: {before: $errors_before, after: $errors_after}, undecodable_alert: $undecodable_alert,
    reconcile_clean: $clean, deleted_row: $lost, reconcile_after_delete: $detected, missing_alert: $missing_alert,
    reconcile_after_restore: $restored, missing_alert_after_restore: $cleared,
    replay_undecodable: $replay_undecodable, replay_missing: $replay_missing, passed: ($failed == 0)}' > "${RESULT}"
echo "result: ${RESULT}"
exit "${FAILED}"
