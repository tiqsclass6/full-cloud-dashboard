#!/usr/bin/env bash
# The reconcile's Kafka offset lookup (kafka_bounds) against a throwaway broker, in the Spark image.
#   usage: ./run-kafka-it.sh
set -euo pipefail
export MSYS_NO_PATHCONV=1
cd "$(dirname "${BASH_SOURCE[0]}")"
IMAGE="lakehouse/spark-jobs:dev"
KAFKA_IMAGE="apache/kafka:4.3.1"   # the version the Java apps' Testcontainers use
NET="kafka-it-$$"
docker build -q -t "${IMAGE}" . >/dev/null
docker network create "${NET}" >/dev/null
cleanup() { docker rm -f kafka-it >/dev/null 2>&1 || true; docker network rm "${NET}" >/dev/null 2>&1 || true; }
trap cleanup EXIT
docker run -d --name kafka-it --network "${NET}" \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka-it:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka-it:9093 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 "${KAFKA_IMAGE}" >/dev/null
for _ in $(seq 1 60); do docker logs kafka-it 2>&1 | grep -q 'Kafka Server started' && break; sleep 2; done
HERE="$(pwd -W 2>/dev/null || pwd)"
docker run --rm --network "${NET}" -v "${HERE}:/work" -w /work --entrypoint bash "${IMAGE}" -c '
  export SPARK_HOME=/opt/spark PYTHONPATH="/work:/opt/spark/python:$(ls /opt/spark/python/lib/py4j-*.zip)"
  status=0; out="$(python3 tests/kafka_bounds_it.py 2>&1)" || status=$?
  grep -E "^(bounds|ranges|KAFKA_BOUNDS_IT|Traceback|AssertionError|[a-zA-Z.]*Exception)" <<<"${out}" || true
  exit "${status}"
'
