#!/usr/bin/env bash
# Create the kind cluster and bring the platform up through GitOps. Idempotent.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require docker kind kubectl helm git

docker info >/dev/null 2>&1 || { echo "Docker is not running." >&2; exit 1; }

log "kind cluster '${CLUSTER_NAME}'"
if kind get clusters 2>/dev/null | grep -qx "${CLUSTER_NAME}"; then
  ok "already exists"
else
  kind create cluster --config "${LOCAL_DIR}/kind-config.yaml"
fi
kc wait --for=condition=Ready nodes --all --timeout=180s

log "Images"
# Per-node pulls of the 400 MB Kafka image stalled, and local dev images cannot be pulled at all.
docker_build -t lakehouse/iceberg-rest:1.10.1-pg "${LOCAL_DIR}/container-images/iceberg-rest" >/dev/null
preload_image lakehouse/iceberg-rest:1.10.1-pg force
for image in "${KAFKA_TOOLS_IMAGE}" "${FLINK_OPERATOR_IMAGE}" "${SPARK_OPERATOR_IMAGE}"; do
  preload_image "${image}"
done
bash "${LOCAL_DIR}/scripts/deploy-ingestion.sh" --build-only
bash "${LOCAL_DIR}/scripts/deploy-flink-job.sh" --build-only
bash "${LOCAL_DIR}/scripts/deploy-spark-jobs.sh"
bash "${LOCAL_DIR}/scripts/deploy-iceberg-exporter.sh" --build-only

bash "${LOCAL_DIR}/scripts/gitops-bootstrap.sh"

log "Done. Run 'make local-smoke' to verify, 'make local-ports' for Argo CD, Grafana and friends."
