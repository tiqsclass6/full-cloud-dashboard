#!/usr/bin/env bash
# Record the resolved-staleness eval scenario (suspend Flink until IcebergTableStale fires); the job always resumes.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require kubectl jq
SCENARIO=resolved-staleness
FD="flinkdeployment/events-to-iceberg"
set_state() { kc -n apps patch "${FD}" --type merge -p "{\"spec\":{\"job\":{\"state\":\"$1\"}}}" >/dev/null; }
trap 'set_state running' EXIT
alert_firing() {
  in_cluster_curl "http://kps-alertmanager.monitoring.svc:9093/api/v2/alerts?active=true" \
    | jq -e '[.[] | select(.labels.alertname == "IcebergTableStale")] | length > 0' >/dev/null
}

log "Suspending the Flink job until IcebergTableStale fires"
set_state suspended
for i in $(seq 1 90); do alert_firing && break; sleep 10; done
alert_firing || { fail "IcebergTableStale did not fire within 15 minutes"; exit 1; }
ok "IcebergTableStale firing after about $((i * 10)) s"
bash "${LOCAL_DIR}/scripts/ai-capture.sh" "${SCENARIO}" IcebergTableStale >/dev/null && ok "alert recorded"

log "Resuming and waiting for fresh commits"
set_state running
for _ in $(seq 1 60); do
  age="$(in_cluster_curl -G --data-urlencode 'query=time() - iceberg_table_last_commit_timestamp_seconds{table="lakehouse.events"}' \
    http://kps-prometheus.monitoring.svc:9090/api/v1/query | jq -r '.data.result[0].value[1] // "9999"')"
  [ "${age%.*}" -lt 60 ] && break
  sleep 10
done
ok "last commit ${age%.*} s ago"
sleep 90    # one more exporter cycle and scrape, so every tool sees the recovered state
bash "${LOCAL_DIR}/scripts/ai-capture.sh" "${SCENARIO}" IcebergTableStale --fixture-only | tail -1
