#!/usr/bin/env bash
# End-to-end checks for the local platform; exits non-zero if any check fails.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require kubectl
FAILED=0

kc -n data rollout status deploy/toolbox --timeout=120s >/dev/null

log "Nodes and zones"
zones=$(kc get nodes -l '!node-role.kubernetes.io/control-plane' \
  -o jsonpath='{range .items[*]}{.metadata.labels.topology\.kubernetes\.io/zone}{"\n"}{end}' | sort -u | wc -l)
[ "$zones" -eq 3 ] && ok "3 worker zones" || fail "expected 3 zones, found $zones"

log "Kafka: replication, rack placement, produce/consume"
kc -n kafka get kafka lakehouse -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' | grep -q True \
  && ok "Kafka CR Ready" || fail "Kafka CR not Ready"
desc=$(kafka_cli kafka-topics.sh --describe --topic events.raw 2>&1) || true
echo "$desc" | head -4
echo "$desc" | grep -q "ReplicationFactor: 3" && ok "events.raw RF=3" || fail "events.raw RF != 3"
echo "$desc" | grep -q "PartitionCount: 6" && ok "events.raw 6 partitions" || fail "events.raw partition count"

# A dedicated topic and a run id keep test messages apart from real events and earlier runs.
run="smoke-$(date +%s)"
for i in $(seq 1 100); do echo "${run}-${i}"; done \
  | kafka_cli kafka-console-producer.sh --topic smoke.test --producer-property acks=all >/dev/null 2>&1 || true
got=$(kafka_cli kafka-console-consumer.sh --topic smoke.test \
  --from-beginning --timeout-ms 15000 2>/dev/null | grep -c "^${run}-" || true)
[ "$got" -eq 100 ] && ok "produced and consumed 100 messages (acks=all)" || fail "consumed $got/100 messages"

log "Flink: Kafka-to-Iceberg job"
# Argo CD can say Healthy while the JobManager crash-loops: check RUNNING and a completed checkpoint.
for _ in $(seq 1 36); do
  [ "$(kc -n apps get flinkdeployment events-to-iceberg -o jsonpath='{.status.jobStatus.state}' 2>/dev/null)" = "RUNNING" ] && break
  sleep 5
done
fstate="$(kc -n apps get flinkdeployment events-to-iceberg -o jsonpath='{.status.jobStatus.state}' 2>/dev/null)"
[ "$fstate" = "RUNNING" ] && ok "job RUNNING" || fail "job state: ${fstate:-missing}"
completed=0
for _ in $(seq 1 12); do
  jid="$(kc -n apps get flinkdeployment events-to-iceberg -o jsonpath='{.status.jobStatus.jobId}' 2>/dev/null)"
  completed="$(in_cluster_curl "http://events-to-iceberg-rest.apps.svc:8081/jobs/${jid}/checkpoints" 2>/dev/null \
    | grep -o '"completed":[0-9]*' | head -1 | cut -d: -f2)"
  [ "${completed:-0}" -gt 0 ] 2>/dev/null && break
  sleep 5
done
[ "${completed:-0}" -gt 0 ] 2>/dev/null && ok "checkpoints completing (${completed})" || fail "no completed checkpoints"

log "S3 (SeaweedFS): buckets"
kc -n data logs job/s3-create-buckets 2>/dev/null | tee /tmp/s3-buckets.txt | sed 's/^/    /'
for b in warehouse checkpoints spark-events; do
  grep -q "$b" /tmp/s3-buckets.txt && ok "bucket $b" || fail "bucket $b missing"
done

log "Iceberg REST catalog (JDBC/Postgres + S3 writes)"
cfg=$(in_cluster_curl http://iceberg-rest:8181/v1/config) || true
echo "$cfg" | grep -q '"endpoints"' && ok "catalog config served" || fail "catalog config: $cfg"
in_cluster_curl -X POST -H 'Content-Type: application/json' \
  -d '{"namespace":["smoke"]}' http://iceberg-rest:8181/v1/namespaces >/dev/null || true
tbl=$(in_cluster_curl -X POST -H 'Content-Type: application/json' \
  -d '{"name":"t","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":true,"type":"long"}]}}' \
  http://iceberg-rest:8181/v1/namespaces/smoke/tables) || true
echo "$tbl" | grep -q "metadata-location" && ok "table created (metadata written to S3)" || fail "create table: ${tbl:0:300}"
in_cluster_curl -X DELETE "http://iceberg-rest:8181/v1/namespaces/smoke/tables/t?purgeRequested=true" >/dev/null || true
in_cluster_curl -X DELETE http://iceberg-rest:8181/v1/namespaces/smoke >/dev/null || true

log "Apicurio schema registry (Confluent-compatible API)"
info=$(in_cluster_curl http://apicurio-registry:8080/apis/registry/v3/system/info) || true
echo "$info" | grep -q '"name"' && ok "registry API up" || fail "registry info: ${info:0:200}"
reg=$(in_cluster_curl -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  -d '{"schema":"{\"type\":\"record\",\"name\":\"Smoke\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}"}' \
  http://apicurio-registry:8080/apis/ccompat/v7/subjects/smoke-value/versions) || true
echo "$reg" | grep -q '"id"' && ok "Avro schema registered" || fail "schema register: ${reg:0:300}"
in_cluster_curl -X DELETE http://apicurio-registry:8080/apis/ccompat/v7/subjects/smoke-value >/dev/null || true

log "Prometheus: targets and Kafka metrics"
# Returns the single scalar result of an instant query, "0" for an empty result, or "ERR".
prom() {
  local out
  out=$(in_cluster_curl -G --data-urlencode "query=$1" http://kps-prometheus.monitoring.svc:9090/api/v1/query) || { echo ERR; return; }
  echo "$out" | grep -q '"status":"success"' || { echo ERR; return; }
  echo "$out" | grep -o '"value":\[[^]]*\]' | grep -o ',"[0-9.]*"' | tr -d ',"' | head -1 | grep . || echo 0
}
# Prometheus needs a scrape interval or two after a fresh install; poll up to ~3 minutes.
prom_wait() {
  local q="$1" v
  for _ in $(seq 1 18); do
    v=$(prom "$q")
    if [ "$v" != "ERR" ] && [ "$v" -gt 0 ] 2>/dev/null; then echo "$v"; return; fi
    sleep 10
  done
  echo "${v:-ERR}"
}
down=$(prom 'count(up == 0)')
if [ "$down" = "0" ]; then ok "no scrape targets down"; else fail "scrape targets down (count: $down)"; fi
kafka_series=$(prom_wait 'count({__name__=~"kafka_.+"})')
if [ "$kafka_series" != "ERR" ] && [ "$kafka_series" -gt 0 ]; then ok "Kafka metrics present ($kafka_series series)"; else fail "no kafka_* metrics in Prometheus"; fi
exporter=$(prom_wait 'count(kafka_topic_partitions)')
if [ "$exporter" != "ERR" ] && [ "$exporter" -gt 0 ]; then ok "kafka-exporter metrics present"; else fail "no kafka-exporter metrics"; fi

echo
if [ "$FAILED" -eq 0 ]; then printf '\033[1;32mAll smoke checks passed.\033[0m\n'; else printf '\033[1;31mSome smoke checks failed.\033[0m\n'; exit 1; fi
