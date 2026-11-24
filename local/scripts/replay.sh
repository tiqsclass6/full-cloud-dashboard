#!/usr/bin/env bash
# Replay records the lakehouse does not have, through the ingestion API (io.lakehouse.flink.ReplayTool).
#   usage: replay.sh missing --partition 3 --offset 276204 [--count 1] [--apply]
#          replay.sh undecodable [--since 2026-11-19T00:00:00Z] [--partition 1 --offset 190353] [--apply]
# Without --apply it prints what it would do. Runs as a one-off pod built from the Flink JobManager's spec: the
# same image, environment, service account (IRSA on AWS) and Kafka client certificate (Read on events.raw only).
# Works on AWS too: KUBE_CONTEXT=lakehouse-dev bash local/scripts/replay.sh ...
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require kubectl jq
[ $# -ge 1 ] || { sed -n 2,4p "$0" >&2; exit 2; }

POD="lakehouse-replay-$(date +%s)"
trap 'kc -n apps delete pod "${POD}" --ignore-not-found --wait=false >/dev/null 2>&1' EXIT
JM="$(kc -n apps get pods -l app=events-to-iceberg,component=jobmanager -o name | head -1)"
[ -n "${JM}" ] || { echo "no Flink JobManager pod in apps" >&2; exit 1; }

ARGS="$(printf '%s\n' "$@" | jq -R . | jq -s .)"
kc -n apps get "${JM}" -o json | jq --arg name "${POD}" --argjson args "${ARGS}" '
  .spec.containers[0] as $c
  # Set at admission (Flink, and on EKS the IRSA webhook, whose token volume is not copied): the replay pod gets its own.
  | ["_POD_IP_ADDRESS", "AWS_ROLE_ARN", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_STS_REGIONAL_ENDPOINTS", "AWS_DEFAULT_REGION"] as $injected
  | ([$c.volumeMounts[] | select(.mountPath | startswith("/etc/kafka-client/"))]) as $mounts
  | {apiVersion: "v1", kind: "Pod",
     metadata: {name: $name, namespace: "apps", labels: {app: "lakehouse-replay"}},
     spec: {
       restartPolicy: "Never",
       serviceAccountName: .spec.serviceAccountName,
       automountServiceAccountToken: false,
       securityContext: .spec.securityContext,
       volumes: [.spec.volumes[] | select(.name as $n | $mounts | map(.name) | index($n))],
       containers: [{
         name: "replay",
         image: $c.image,
         imagePullPolicy: "IfNotPresent",
         command: (["java", "-Xmx512m", "-cp", "/opt/flink/usrlib/job.jar:/opt/flink/lib/*", "io.lakehouse.flink.ReplayTool"] + $args),
         env: ([$c.env[] | select(.name | IN($injected[]) | not)] + [{name: "INGEST_URL", value: "http://ingestion-service.apps.svc:8080/v1/events"}]),
         volumeMounts: $mounts,
         securityContext: $c.securityContext,
         resources: {requests: {cpu: "200m", memory: "768Mi"}, limits: {memory: "768Mi"}}
       }]
     }}' | kc apply -f - >/dev/null

for _ in $(seq 1 120); do
  phase="$(kc -n apps get pod "${POD}" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  case "${phase}" in Succeeded|Failed) break ;; esac
  sleep 2
done
# The tool prints JSON lines and a SUMMARY line; the JVM's own log lines are left out unless it failed.
LOGS="$(kc -n apps logs "${POD}" 2>&1)"
grep -E '^(\{|SUMMARY )' <<<"${LOGS}" || true
grep -E '^(ingestion API|usage)' <<<"${LOGS}" >&2 || true
if [ "${phase}" != "Succeeded" ]; then
  echo "replay pod ${phase:-did not finish}; last log lines:" >&2
  grep -v -E '^(\{|SUMMARY )' <<<"${LOGS}" | tail -25 >&2
  exit 1
fi
