#!/usr/bin/env bash
# Build the Iceberg table-health exporter, load the image into kind and restart it.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require docker kind kubectl

APP_DIR="$(cd "${LOCAL_DIR}/../apps/iceberg-exporter" && pwd)"
IMAGE="lakehouse/iceberg-exporter:dev"

log "Building (Gradle: tests + distribution)"
if [ -z "${JAVA_HOME:-}" ] || [ ! -x "${JAVA_HOME}/bin/java" ]; then
  for candidate in "/c/Program Files/OpenJDK/jdk-25" "/c/Program Files/OpenJDK/jdk-27" "/usr/lib/jvm/java-21-openjdk-amd64"; do
    [ -x "${candidate}/bin/java" ] && export JAVA_HOME="${candidate}" && break
  done
fi
(cd "${APP_DIR}" && ./gradlew build installDist --no-daemon -q)

log "Building image ${IMAGE}"
(cd "${APP_DIR}" && docker_build -t "${IMAGE}" .) >/dev/null
preload_image "${IMAGE}" force

[ "${1:-}" = "--build-only" ] && exit 0

log "Rolling out the new image"
# The tag dev does not change locally, so a restart picks up the new image.
kc -n apps rollout restart deployment/iceberg-exporter >/dev/null 2>&1 || true
kc -n apps rollout status deployment/iceberg-exporter --timeout=300s 2>/dev/null || true
