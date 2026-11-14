#!/usr/bin/env bash
# Build the Flink job, load its image into kind and (re)deploy the FlinkDeployment.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require docker kind kubectl

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
kc -n apps patch flinkdeployment events-to-iceberg --type merge \
  -p "{\"spec\":{\"restartNonce\":$(date +%s)}}" >/dev/null

log "Waiting for the job to be RUNNING"
for _ in $(seq 1 90); do
  state="$(kc -n apps get flinkdeployment events-to-iceberg -o jsonpath='{.status.jobStatus.state}' 2>/dev/null || true)"
  [ "${state}" = "RUNNING" ] && break
  sleep 5
done
kc -n apps get flinkdeployment events-to-iceberg
[ "${state:-}" = "RUNNING" ] || { echo "job is not RUNNING (state: ${state:-unknown})" >&2; exit 1; }
