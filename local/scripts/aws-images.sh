#!/usr/bin/env bash
# Build the platform images and push them to the environment's ECR, plus a linux/arm64 agent image for AgentCore.
#   usage: aws-images.sh [env]     (default dev)
ENV_NAME="${1:-dev}"
GITOPS_ENV="${ENV_NAME}" KUBE_CONTEXT="lakehouse-${ENV_NAME}"   # not kind: the build scripts skip loading into kind
export GITOPS_ENV KUBE_CONTEXT
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require docker aws terraform

REGISTRY="$(terraform -chdir="${REPO_ROOT}/terraform/envs/${ENV_NAME}" output -raw ecr_registry)"
REGION="$(terraform -chdir="${REPO_ROOT}/terraform/envs/${ENV_NAME}" output -raw region)"
TAG="$(grep -E '^\s+imageTag:' "${REPO_ROOT}/helm/values/${ENV_NAME}.yaml" | head -1 | awk '{print $2}' | tr -d '"')"
[ -n "${TAG}" ] || { echo "no global.imageTag in helm/values/${ENV_NAME}.yaml" >&2; exit 1; }

bash "${LOCAL_DIR}/scripts/deploy-ingestion.sh" --build-only
bash "${LOCAL_DIR}/scripts/deploy-flink-job.sh" --build-only
bash "${LOCAL_DIR}/scripts/deploy-spark-jobs.sh"
bash "${LOCAL_DIR}/scripts/deploy-iceberg-exporter.sh" --build-only
bash "${LOCAL_DIR}/scripts/deploy-ai-ops.sh" --build-only

log "Security gate (vulnerabilities in our layers, IaC misconfigurations)"
bash "${LOCAL_DIR}/scripts/security-scan.sh" dev || { echo "security gate failed: not pushing" >&2; exit 1; }

# Signed by digest with the environment's KMS key; signatures stay in ECR (no transparency log).
SIGNING_KEY="$(terraform -chdir="${REPO_ROOT}/terraform/envs/${ENV_NAME}" output -raw image_signing_key)"
COSIGN="$(cosign_bin)"

sign_and_verify() {  # sign_and_verify <image reference by digest> <label>
  # Cosign 3 defaults to the transparency log: turn it off and write the classic format.
  if ! out="$("${COSIGN}" sign --yes --key "${SIGNING_KEY}" --use-signing-config=false --tlog-upload=false \
               --new-bundle-format=false --registry-referrers-mode=legacy "$1" 2>&1)"; then
    echo "signing failed for $1:" >&2; echo "${out}" | tail -3 >&2; exit 1
  fi
  "${COSIGN}" verify --key "${SIGNING_KEY}" --insecure-ignore-tlog=true "$1" >/dev/null 2>&1 \
    && ok "$2 pushed, signed and verified (${1##*@})" \
    || { echo "signature verification failed for $1" >&2; exit 1; }
}

log "Pushing to ${REGISTRY} (tag ${TAG}) and signing"
aws ecr get-login-password --region "${REGION}" | docker login --username AWS --password-stdin "${REGISTRY}" >/dev/null
for name in ingestion-service flink-iceberg-job spark-jobs iceberg-exporter mcp-platform-tools ai-ops hermes; do
  docker tag "lakehouse/${name}:dev" "${REGISTRY}/lakehouse/${name}:${TAG}"
  docker push -q "${REGISTRY}/lakehouse/${name}:${TAG}" >/dev/null
  ref="$(docker image inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "${REGISTRY}/lakehouse/${name}:${TAG}" \
         | grep "^${REGISTRY}/lakehouse/${name}@" | head -1)"
  sign_and_verify "${ref}" "lakehouse/${name}:${TAG}"
done

# The agent for AgentCore Runtime: the same image built for linux/arm64 (emulated on an x86 machine).
ARM_TAG="${TAG}-arm64"
log "Building lakehouse/ai-ops:${ARM_TAG} (linux/arm64, for AgentCore Runtime)"
docker buildx build -q --platform linux/arm64 --provenance=false -t "${REGISTRY}/lakehouse/ai-ops:${ARM_TAG}" \
  --push "${REPO_ROOT}/apps/ai-ops" >/dev/null
digest="$(aws ecr describe-images --region "${REGION}" --repository-name lakehouse/ai-ops \
          --image-ids imageTag="${ARM_TAG}" --query 'imageDetails[0].imageDigest' --output text | tr -d '\r')"
sign_and_verify "${REGISTRY}/lakehouse/ai-ops@${digest}" "lakehouse/ai-ops:${ARM_TAG}"
