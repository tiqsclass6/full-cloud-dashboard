#!/usr/bin/env bash
# Run one PySpark job on the cluster as a SparkApplication, wait for it, print its SUMMARY line.
#   usage: spark-run.sh <name> <maintenance|aggregate|reconcile> [job args...]
#   env:   EXECUTORS (default 2)   EXECUTOR_MEMORY (default 1g)   TIMEOUT_SECONDS (default 900)   IMAGE
#          SPARK_CONF_EXTRA  extra Spark settings, one "key=value" per line
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require kubectl

NAME="${1:?name required}"; ENTRY="${2:?maintenance|aggregate|reconcile}"; shift 2
EXECUTORS="${EXECUTORS:-2}"; EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-1g}"; TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-900}"

args_yaml=""
for a in "$@"; do args_yaml+="    - \"${a}\""$'\n'; done
extra_yaml=""
while IFS='=' read -r k v; do [ -n "${k:-}" ] && extra_yaml+="    ${k}: \"${v}\""$'\n'; done <<< "${SPARK_CONF_EXTRA:-}"

kc -n apps delete sparkapplication "${NAME}" --ignore-not-found --wait=true >/dev/null
kc -n apps apply -f - >/dev/null <<YAML
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: ${NAME}
  namespace: apps
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: ${IMAGE:-lakehouse/spark-jobs:dev}
  imagePullPolicy: IfNotPresent
  mainApplicationFile: local:///opt/jobs/${ENTRY}.py
  arguments:
${args_yaml:-    []
}  sparkVersion: "4.1.4"
  restartPolicy:
    type: Never
  timeToLiveSeconds: 3600
  sparkConf:
    spark.kubernetes.driverEnv.ICEBERG_REST_URI: "http://iceberg-rest.data.svc:8181"
    spark.kubernetes.driverEnv.ICEBERG_WAREHOUSE: "s3://warehouse/"
    spark.kubernetes.driverEnv.S3_ENDPOINT: "http://s3.data.svc:8333"
    spark.kubernetes.driver.secretKeyRef.S3_ACCESS_KEY_ID: "s3-credentials:AWS_ACCESS_KEY_ID"
    spark.kubernetes.driver.secretKeyRef.S3_SECRET_ACCESS_KEY: "s3-credentials:AWS_SECRET_ACCESS_KEY"
${extra_yaml}  # Pod Security "restricted" (the apps namespace enforces it), set through Spark pod templates as in
  # helm/charts/spark-jobs: the operator's own securityContext fields need its disabled webhook.
  driver:
    cores: 1
    memory: 1g
    serviceAccount: spark-operator-spark
    template:
      spec:
        securityContext: {runAsNonRoot: true, runAsUser: 185, seccompProfile: {type: RuntimeDefault}}
        containers:
          - name: spark-kubernetes-driver
            securityContext: {allowPrivilegeEscalation: false, readOnlyRootFilesystem: false, capabilities: {drop: ["ALL"]}}
  executor:
    instances: ${EXECUTORS}
    cores: 1
    memory: ${EXECUTOR_MEMORY}
    serviceAccount: spark-operator-spark
    template:
      spec:
        securityContext: {runAsNonRoot: true, runAsUser: 185, seccompProfile: {type: RuntimeDefault}}
        containers:
          - name: spark-kubernetes-executor
            securityContext: {allowPrivilegeEscalation: false, readOnlyRootFilesystem: false, capabilities: {drop: ["ALL"]}}
YAML

echo "submitted ${NAME}; waiting"
state=""
for _ in $(seq 1 $((TIMEOUT_SECONDS / 3))); do
  state="$(kc -n apps get sparkapplication "${NAME}" -o jsonpath='{.status.applicationState.state}' 2>/dev/null || true)"
  case "${state}" in COMPLETED|FAILED|SUBMISSION_FAILED) break ;; esac
  sleep 3
done
echo "state: ${state:-unknown}"
driver="${NAME}-driver"
kc -n apps logs "${driver}" 2>/dev/null | grep '^SUMMARY ' | tail -1 | sed 's/^SUMMARY //' || true
if [ "${state}" != "COMPLETED" ]; then
  kc -n apps get sparkapplication "${NAME}" -o jsonpath='{.status.applicationState.errorMessage}' 2>/dev/null; echo
  kc -n apps logs "${driver}" --tail=25 2>/dev/null || true
  exit 1
fi
