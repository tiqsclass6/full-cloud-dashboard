#!/usr/bin/env bash
# Network policy test with real TCP connects from apps, a non-platform namespace and the toolbox (open = handshake in 4 s).
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require kubectl
FAILED=0
PROBE="netpol-probe"
OUTSIDE_NS="netpol-test"
trap 'kc -n apps delete pod "${PROBE}" --ignore-not-found --wait=false >/dev/null 2>&1;
      kc delete namespace "${OUTSIDE_NS}" --ignore-not-found --wait=false >/dev/null 2>&1' EXIT

probe_pod() {  # probe_pod <namespace>: a small pod with bash, compliant with Pod Security "restricted"
  kc apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${PROBE}
  namespace: $1
  labels: {app: ${PROBE}}
spec:
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
    runAsUser: 1001
    seccompProfile: {type: RuntimeDefault}
  containers:
    - name: probe
      image: ${KAFKA_TOOLS_IMAGE}
      command: ["sleep", "600"]
      resources:
        requests: {cpu: 10m, memory: 32Mi}
        limits: {memory: 64Mi}
      securityContext:
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
        capabilities: {drop: ["ALL"]}
EOF
  kc -n "$1" wait --for=condition=Ready "pod/${PROBE}" --timeout=120s >/dev/null
}

connects() {  # connects <namespace> <pod> <host> <port>: exit 0 if a TCP connection opens
  kc -n "$1" exec "$2" -- timeout 4 bash -c "exec 3<>/dev/tcp/$3/$4" >/dev/null 2>&1
}
expect_open()    { if connects "$1" "$2" "$3" "$4"; then ok "$5"; else fail "$5 (blocked)"; fi; }
expect_blocked() { if connects "$1" "$2" "$3" "$4"; then fail "$5 (open)"; else ok "$5"; fi; }
# Services that exist only locally (stand-ins for managed AWS services) are skipped elsewhere.
exists() { kc -n "${2}" get svc "${1}" >/dev/null 2>&1; }
skip() { printf '  --  %s (not in this environment)\n' "$1"; }

KAFKA=lakehouse-kafka-bootstrap.kafka.svc
APICURIO=apicurio-registry.data.svc
CATALOG=iceberg-rest.data.svc
S3=s3.data.svc
PG=postgres.data.svc
PROM=kps-prometheus.monitoring.svc
INGEST=ingestion-service.apps.svc

log "From a client pod in apps"
kc -n apps delete pod "${PROBE}" --ignore-not-found --wait=true >/dev/null
probe_pod apps
expect_open    apps "${PROBE}" "${KAFKA}" 9093    "Kafka TLS listener"
expect_open    apps "${PROBE}" "${APICURIO}" 8080 "schema registry"
if exists iceberg-rest data; then expect_open apps "${PROBE}" "${CATALOG}" 8181 "Iceberg REST catalog"; else skip "Iceberg REST catalog"; fi
if exists s3 data; then expect_open apps "${PROBE}" "${S3}" 8333 "S3 (SeaweedFS)"; else skip "S3 (SeaweedFS)"; fi
expect_open    apps "${PROBE}" kubernetes.default.svc 443 "Kubernetes API"
if exists postgres data; then expect_blocked apps "${PROBE}" "${PG}" 5432 "Postgres (only the catalog and registry use it)"; else skip "Postgres"; fi
expect_blocked apps "${PROBE}" "${PROM}" 9090     "Prometheus"
expect_blocked apps "${PROBE}" 1.1.1.1 80         "plain HTTP to the internet"
expect_blocked apps "${PROBE}" 169.254.169.254 80 "instance metadata (pods must use IRSA)"

log "From a namespace outside the platform"
kc create namespace "${OUTSIDE_NS}" --dry-run=client -o yaml | kc apply -f - >/dev/null
probe_pod "${OUTSIDE_NS}"
expect_blocked "${OUTSIDE_NS}" "${PROBE}" "${INGEST}" 8080   "ingestion service"
expect_blocked "${OUTSIDE_NS}" "${PROBE}" "${APICURIO}" 8080 "schema registry"
expect_blocked "${OUTSIDE_NS}" "${PROBE}" "${KAFKA}" 9093    "Kafka TLS listener"
if exists postgres data; then expect_blocked "${OUTSIDE_NS}" "${PROBE}" "${PG}" 5432 "Postgres"; else skip "Postgres"; fi

log "From the diagnostics toolbox (data)"
toolbox="$(kc -n data get pods -l app=toolbox -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
[ -n "${toolbox}" ] || skip "diagnostics toolbox"
for target in $( [ -n "${toolbox}" ] && echo "${INGEST}:8080 ${PROM}:9090 ${APICURIO}:8080" ); do
  set -- ${target/:/ }
  code=0; kc -n data exec "${toolbox}" -- curl -s -o /dev/null --connect-timeout 4 -m 5 "http://$1:$2/" || code=$?
  # 0 or an HTTP-level error means the connection opened; 7 (refused) or 28 (timeout) means it did not.
  if [ "${code}" -ne 7 ] && [ "${code}" -ne 28 ]; then ok "toolbox reaches $1:$2"; else fail "toolbox reaches $1:$2 (curl exit ${code})"; fi
done

echo
if [ "${FAILED}" -eq 0 ]; then log "All network policy checks passed."; else log "Some network policy checks FAILED"; exit 1; fi
