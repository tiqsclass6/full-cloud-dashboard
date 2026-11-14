#!/usr/bin/env bash
# Run the in-cluster load generator against the ingestion service and print its RESULT line.
#   usage: loadtest.sh [rate events/s] [duration s] [warmup s] [batch] [workers]
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require kubectl

RATE="${1:-2000}"; DURATION="${2:-60}"; WARMUP="${3:-10}"; BATCH="${4:-10}"; WORKERS="${5:-64}"
JOB="loadgen-$(date +%s)"

kc -n apps apply -f - >/dev/null <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: ${JOB}
  namespace: apps
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 600
  template:
    spec:
      restartPolicy: Never
      securityContext:          # Pod Security "restricted" (enforced in apps)
        runAsNonRoot: true
        runAsUser: 10001
        seccompProfile: {type: RuntimeDefault}
      containers:
        - name: loadgen
          image: lakehouse/ingestion-service:dev
          imagePullPolicy: IfNotPresent
          command: ["/opt/loadgen/bin/loadgen"]
          args: ["--url", "http://ingestion-service.apps.svc:8080/v1/events", "--rate", "${RATE}",
                 "--duration", "${DURATION}", "--warmup", "${WARMUP}", "--batch", "${BATCH}", "--workers", "${WORKERS}"]
          resources:
            requests: {cpu: 500m, memory: 256Mi}
            limits: {memory: 768Mi}
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities: {drop: ["ALL"]}
EOF

kc -n apps wait --for=condition=complete "job/${JOB}" --timeout="$((DURATION + 120))s" >/dev/null \
  || { kc -n apps logs "job/${JOB}" | tail -20; echo "load test did not complete" >&2; exit 1; }
kc -n apps logs "job/${JOB}" | grep -E "^(loadgen|RESULT)"
