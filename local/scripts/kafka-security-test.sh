#!/usr/bin/env bash
# Kafka ACLs and mTLS, tested from apps with the real client certificates (never writes to events.raw).
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require kubectl
FAILED=0
POD="kafka-security-test"
BOOTSTRAP="lakehouse-kafka-bootstrap.kafka.svc:9093"
trap 'kc -n apps delete pod "${POD}" --ignore-not-found --wait=false >/dev/null 2>&1' EXIT

log "Test pod"
kc -n apps delete pod "${POD}" --ignore-not-found --wait=true >/dev/null
kc apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${POD}
  namespace: apps
  labels: {app: ${POD}}
spec:
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
    runAsUser: 1001
    seccompProfile: {type: RuntimeDefault}
  containers:
    - name: kafka
      image: ${KAFKA_TOOLS_IMAGE}
      command: ["sleep", "600"]
      env:
        - {name: LOG_DIR, value: /tmp/logs}
      resources:
        requests: {cpu: 50m, memory: 256Mi}
        limits: {memory: 768Mi}
      securityContext:
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
        capabilities: {drop: ["ALL"]}
      volumeMounts:
        - {name: ingest, mountPath: /certs/ingestion-service, readOnly: true}
        - {name: flink, mountPath: /certs/events-to-iceberg, readOnly: true}
        - {name: ca, mountPath: /certs/ca, readOnly: true}
        - {name: tmp, mountPath: /tmp}
  volumes:
    - {name: ingest, secret: {secretName: ingestion-service}}
    - {name: flink, secret: {secretName: events-to-iceberg}}
    - {name: ca, secret: {secretName: lakehouse-cluster-ca-cert}}
    - {name: tmp, emptyDir: {}}
EOF
kc -n apps wait --for=condition=Ready "pod/${POD}" --timeout=180s >/dev/null && ok "ready" || { fail "test pod not ready"; exit 1; }

# Client settings per identity, written inside the pod from the mounted secrets.
kc -n apps exec "${POD}" -- bash -c '
  for u in ingestion-service events-to-iceberg; do
    printf "%s\n" "security.protocol=SSL" "ssl.keystore.type=PKCS12" \
      "ssl.keystore.location=/certs/$u/user.p12" "ssl.keystore.password=$(cat /certs/$u/user.password)" \
      "ssl.truststore.type=PKCS12" "ssl.truststore.location=/certs/ca/ca.p12" \
      "ssl.truststore.password=$(cat /certs/ca/ca.password)" > /tmp/$u.properties
  done
  printf "%s\n" "security.protocol=SSL" "ssl.truststore.type=PKCS12" "ssl.truststore.location=/certs/ca/ca.p12" \
    "ssl.truststore.password=$(cat /certs/ca/ca.password)" > /tmp/no-client-cert.properties'

run() {  # run <identity> <tool> [args...]: prints the tool output (stdout and stderr)
  local who="$1" tool="$2"; shift 2
  local flag=--command-config
  [ "${tool}" = kafka-console-producer.sh ] && flag=--producer.config
  [ "${tool}" = kafka-console-consumer.sh ] && flag=--consumer.config
  kc -n apps exec -i "${POD}" -- timeout 60 "/opt/kafka/bin/${tool}" --bootstrap-server "${BOOTSTRAP}" \
    "${flag}" "/tmp/${who}.properties" "$@" 2>&1 || true
}
expect() {  # expect <description> <regex> <output>
  if echo "$3" | grep -qE "$2"; then ok "$1"; else fail "$1"; echo "$3" | tail -3 | sed 's/^/        /'; fi
}

log "ingestion-service (Write, Describe on events.raw)"
# kafka-get-offsets needs only Describe (kafka-topics --describe also reads configs: DescribeConfigs).
expect "can describe events.raw (offsets)" "^events\.raw:0:[0-9]+" \
  "$(run ingestion-service kafka-get-offsets.sh --topic events.raw)"
expect "cannot read events.raw" "TopicAuthorizationException|GroupAuthorizationException|Not authorized" \
  "$(run ingestion-service kafka-console-consumer.sh --topic events.raw --group acl-test --max-messages 1 --timeout-ms 15000)"
expect "cannot write smoke.test" "TopicAuthorizationException|Not authorized" \
  "$(echo denied | run ingestion-service kafka-console-producer.sh --topic smoke.test --producer-property acks=all)"
expect "cannot create topics" "TopicAuthorizationException|ClusterAuthorizationException|Authorization failed" \
  "$(run ingestion-service kafka-topics.sh --create --topic acl-test --partitions 1 --replication-factor 1)"

log "events-to-iceberg (Read, Describe on events.raw; its own consumer group)"
# Refused before the topic check: an idempotent producer needs IdempotentWrite or Write somewhere.
expect "cannot write events.raw" "TopicAuthorizationException|ClusterAuthorizationException|Not authorized" \
  "$(echo denied | run events-to-iceberg kafka-console-producer.sh --topic events.raw --producer-property acks=all)"
expect "cannot use another consumer group" "GroupAuthorizationException|Not authorized" \
  "$(run events-to-iceberg kafka-console-consumer.sh --topic events.raw --group not-flink --max-messages 1 --timeout-ms 15000)"
expect "can describe its own consumer group" "events\.raw" \
  "$(run events-to-iceberg kafka-consumer-groups.sh --describe --group flink-events-to-iceberg)"

log "Unauthenticated access"
expect "TLS without a client certificate is rejected" "SslAuthenticationException|SSL handshake failed|Failed authentication|TimeoutException" \
  "$(run no-client-cert kafka-topics.sh --list)"
plain="$(kc -n apps exec "${POD}" -- timeout 10 bash -c 'exec 3<>/dev/tcp/lakehouse-kafka-bootstrap.kafka.svc/9092 && echo open' 2>&1 || true)"
[[ "${plain}" != *open* ]] && ok "no plaintext listener on 9092" || fail "port 9092 is open"

echo
if [ "${FAILED}" -eq 0 ]; then log "All Kafka security checks passed."; else log "Some Kafka security checks FAILED"; exit 1; fi
