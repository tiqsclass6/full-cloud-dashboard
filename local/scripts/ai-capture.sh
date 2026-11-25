#!/usr/bin/env bash
# Record an eval fixture from a live incident (evals/agent-scenarios).
#   ai-capture.sh <scenario> <alertname[,alertname...]>   the alert payload and every tool's answer now
#   ai-capture.sh --merge <scenario>                       add the latest live investigation's tool calls
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require kubectl jq
DIR="${REPO_ROOT}/evals/agent-scenarios"
mkdir -p "${DIR}/alerts" "${DIR}/fixtures"

if [ "${1:-}" = "--merge" ]; then
  SCENARIO="$2"
  # SQL string literals: the quote comes in as $q (inside the single-quoted filter the shell would swallow it).
  NAMES="$(jq -r --arg q "'" '[.alerts[].labels.alertname] | unique | map($q + . + $q) | join(",")' "${DIR}/alerts/${SCENARIO}.json" | tr -d '\r')"
  INV="$(kc -n data exec statefulset/postgres -- psql -U platform -d platform -tAc \
    "select id from ops.investigations where model <> 'test' and trigger->'alerts'->0->'labels'->>'alertname' in (${NAMES})
     order by started_at desc limit 1" | tr -d '\r')"
  [ -n "${INV}" ] || { echo "no live investigation for ${NAMES}" >&2; exit 1; }
  CALLS="$(kc -n data exec statefulset/postgres -- psql -U platform -d platform -tAc \
    "select coalesce(json_agg(json_build_object('tool', tool, 'arguments', arguments, 'result', result, 'is_error', is_error) order by seq), '[]')
     from ops.tool_calls where investigation_id = '${INV}'" | tr -d '\r')"
  TMP="$(mktemp -p "${REPO_ROOT}/build" fixture.XXXXXX)"
  jq --argjson calls "${CALLS}" --arg inv "${INV}" \
    '.responses += [$calls[] | select(. as $c | ([.tool, .arguments]) as $k | all(.responses[]?; [.tool, .arguments] != $k))]
     | .merged_investigation = $inv' "${DIR}/fixtures/${SCENARIO}.json" > "${TMP}" && mv "${TMP}" "${DIR}/fixtures/${SCENARIO}.json"
  ok "merged $(printf '%s' "${CALLS}" | jq length) recorded calls from investigation ${INV}"
  exit 0
fi

SCENARIO="${1:?scenario name}"; ALERTS="${2:?alertname[,alertname...]}"
log "Capturing ${SCENARIO} (${ALERTS})"
# --fixture-only: keep the earlier alert, snapshot the tools now (an alert that has since resolved).
if [ "${3:-}" = "--fixture-only" ]; then
  [ -f "${DIR}/alerts/${SCENARIO}.json" ] || { echo "no recorded alert for ${SCENARIO}" >&2; exit 1; }
else
in_cluster_curl "http://kps-alertmanager.monitoring.svc:9093/api/v2/alerts?active=true&silenced=false" \
  | jq --arg names "${ALERTS}" --arg scenario "${SCENARIO}" '
      ($names | split(",")) as $n
      | [.[] | select(.labels.alertname as $a | $n | index($a))] as $a
      | {receiver: "ops-agent", status: "firing", groupKey: ("capture/" + $scenario),
         commonLabels: ($a[0].labels // {} | {alertname, severity}),
         alerts: [$a[] | {status: "firing", labels, annotations, startsAt, fingerprint}]}' \
  > "${DIR}/alerts/${SCENARIO}.json"
COUNT="$(jq '.alerts | length' "${DIR}/alerts/${SCENARIO}.json")"
[ "${COUNT}" -gt 0 ] || { echo "none of ${ALERTS} is firing now" >&2; exit 1; }
ok "${COUNT} firing alert(s)"
fi
kc -n ai-ops exec -i deploy/ops-agent -- python -m ops_agent capture --alert - --out - \
    --source "live capture during ${SCENARIO}, $(date -u +%Y-%m-%dT%H:%MZ)" \
  < "${DIR}/alerts/${SCENARIO}.json" > "${DIR}/fixtures/${SCENARIO}.json"
ok "fixture: $(jq '.responses | length' "${DIR}/fixtures/${SCENARIO}.json") tool results"
