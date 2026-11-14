#!/usr/bin/env bash
# Build the ingestion service, load the image into kind and deploy it.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require docker kind kubectl

APP_DIR="$(cd "${LOCAL_DIR}/../apps/ingestion-service" && pwd)"
IMAGE="lakehouse/ingestion-service:dev"

log "Building (Gradle: tests + distributions)"
# The Gradle build needs a JDK 17+ to run itself; it provisions JDK 21 for compiling via toolchains.
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
kc -n apps rollout restart deployment/ingestion-service >/dev/null
kc -n apps rollout status deployment/ingestion-service --timeout=300s
