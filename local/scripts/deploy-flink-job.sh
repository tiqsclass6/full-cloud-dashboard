#!/usr/bin/env bash
# Build the Flink job, load its image into kind and (re)deploy the FlinkDeployment.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require docker kind kubectl jq

APP_DIR="$(cd "${LOCAL_DIR}/../apps/flink-iceberg-job" && pwd)"
IMAGE="lakehouse/flink-iceberg-job:dev"

log "Building the job (Gradle: tests + fat jar)"
if [ -z "${JAVA_HOME:-}" ] || [ ! -x "${JAVA_HOME}/bin/java" ]; then
  for candidate in "/c/Program Files/OpenJDK/jdk-25" "/c/Program Files/OpenJDK/jdk-27" "/usr/lib/jvm/java-21-openjdk-amd64"; do
    [ -x "${candidate}/bin/java" ] && export JAVA_HOME="${candidate}" && break
  done
fi
# Gradle misses JDKs nested in its cache (the job needs Java 17), so pass them explicitly.
jdks=()
for dir in "${HOME}"/.gradle/jdks/*/ "${HOME}"/.gradle/jdks/*/*/; do
  dir="${dir%/}"
  [ -x "${dir}/bin/java" ] || [ -x "${dir}/bin/java.exe" ] || continue
  command -v cygpath >/dev/null && dir="$(cygpath -w "${dir}")"
  jdks+=("${dir}")
done
toolchains=()
[ "${#jdks[@]}" -gt 0 ] && toolchains=("-Porg.gradle.java.installations.paths=$(IFS=,; echo "${jdks[*]}")")
(cd "${APP_DIR}" && ./gradlew build --no-daemon -q "${toolchains[@]}")

log "Building image ${IMAGE}"
(cd "${APP_DIR}" && docker_build -t "${IMAGE}" .) >/dev/null
preload_image "${IMAGE}" force

[ "${1:-}" = "--build-only" ] && exit 0

log "Restarting the job on the new image"
# The tag dev does not change locally: bump restartNonce (ignored by Argo CD) to restart via a savepoint.
NONCE="$(date +%s)"
kc -n apps patch flinkdeployment events-to-iceberg --type merge \
  -p "{\"spec\":{\"restartNonce\":${NONCE}}}" >/dev/null

log "Waiting for the restart (nonce ${NONCE}) to finish"
# RUNNING alone is not enough: the old job is RUNNING until the operator acts on the patch. Done means the
# operator recorded this nonce in its last stable spec, and the job is RUNNING and the deployment STABLE.
stable_nonce() {
  kc -n apps get flinkdeployment events-to-iceberg -o jsonpath='{.status.reconciliationStatus.lastStableSpec}' \
    2>/dev/null | jq -r '.spec.restartNonce // empty' 2>/dev/null || true
}
finished="" last=""
for _ in $(seq 1 120); do
  status="$(kc -n apps get flinkdeployment events-to-iceberg \
    -o jsonpath='{.status.jobStatus.state} {.status.lifecycleState}' 2>/dev/null || true)"
  [ "${status}" != "${last}" ] && echo "  ${status}" && last="${status}"
  if [ "${status}" = "RUNNING STABLE" ] && [ "$(stable_nonce)" = "${NONCE}" ]; then finished=1; break; fi
  sleep 5
done
kc -n apps get flinkdeployment events-to-iceberg
[ -n "${finished}" ] || { echo "restart not finished after 10 minutes (status: ${status:-unknown}, stable nonce: $(stable_nonce))" >&2; exit 1; }
ok "job restarted on the new image (nonce ${NONCE})"
