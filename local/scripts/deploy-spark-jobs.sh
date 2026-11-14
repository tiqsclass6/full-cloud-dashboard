#!/usr/bin/env bash
# Build the PySpark job image (Iceberg runtime baked in) and load it into kind.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require docker kind kubectl

APP_DIR="$(cd "${LOCAL_DIR}/../apps/spark-jobs" && pwd)"
IMAGE="lakehouse/spark-jobs:dev"

log "Building ${IMAGE} (base: ${SPARK_IMAGE})"
(cd "${APP_DIR}" && docker_build -t "${IMAGE}" .) >/dev/null
preload_image "${IMAGE}" force
# The scheduled jobs themselves come from Git (helm/charts/spark-jobs); new runs use the new image.
