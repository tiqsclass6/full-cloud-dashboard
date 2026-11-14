#!/usr/bin/env bash
# pytest inside the Spark image: Hadoop needs winutils on Windows, so not on the host.
#   usage: ./run-tests.sh [pytest args]     e.g. ./run-tests.sh -k aggregate -x
set -euo pipefail
export MSYS_NO_PATHCONV=1
cd "$(dirname "${BASH_SOURCE[0]}")"
IMAGE="lakehouse/spark-jobs:dev"
docker build -q -t "${IMAGE}" . >/dev/null
HERE="$(pwd -W 2>/dev/null || pwd)"
# Mounts the working copy over the baked-in code; args pass through as positional parameters.
docker run --rm -v "${HERE}:/work" -w /work --entrypoint bash "${IMAGE}" -c '
  pip install -q --no-cache-dir --target /tmp/py pytest >/dev/null 2>&1
  export SPARK_HOME=/opt/spark
  export PYTHONPATH="/work:/tmp/py:/opt/spark/python:$(ls /opt/spark/python/lib/py4j-*.zip)"
  python3 -m pytest -p no:cacheprovider "$@"
' _ "$@"
