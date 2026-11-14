#!/usr/bin/env bash
# Bring the platform up on an applied AWS environment: kubeconfig, values, images, StorageClass, GitOps bootstrap.
#   usage: aws-up.sh [env]     (default dev)
ENV_NAME="${1:-dev}"
GITOPS_ENV="${ENV_NAME}" KUBE_CONTEXT="lakehouse-${ENV_NAME}"
export GITOPS_ENV KUBE_CONTEXT
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require aws terraform kubectl helm docker openssl

TF_DIR="${REPO_ROOT}/terraform/envs/${ENV_NAME}"
tfout() { terraform -chdir="${TF_DIR}" output -raw "$1"; }
REGION="$(tfout region)"

log "Preflight: Docker and the ECR login (fail now, not after seven images were built)"
# The arm64 agent image needs QEMU registered in Docker's VM on x86 (a one-time, privileged step).
BINFMT_IMAGE="${BINFMT_IMAGE:-tonistiigi/binfmt@sha256:400a4873b838d1b89194d982c45e5fb3cda4593fbfd7e08a02e76b03b21166f0}"   # :latest of 2026-06-05, pinned by digest
docker info >/dev/null 2>&1 || { echo "Docker is not running: start Docker Desktop and run this again" >&2; exit 1; }
arm64_runs() { [ "$(docker run --rm --platform linux/arm64 alpine uname -m 2>/dev/null | tr -d '\r')" = aarch64 ]; }
if arm64_runs; then
  ok "Docker can run linux/arm64"
else
  echo "  linux/arm64 emulation is not registered: running ${BINFMT_IMAGE} --install arm64 (privileged)"
  docker run --privileged --rm "${BINFMT_IMAGE}" --install arm64 >/dev/null
  arm64_runs && ok "linux/arm64 emulation registered" \
    || { echo "linux/arm64 still does not run; see docs/lab/RUNBOOK.md Part 9 step 4" >&2; exit 1; }
fi
aws ecr get-login-password --region "${REGION}" | docker login --username AWS --password-stdin "$(tfout ecr_registry)" >/dev/null \
  && ok "logged in to ECR" || { echo "ECR login failed: check your AWS credentials (aws sts get-caller-identity)" >&2; exit 1; }

log "kubeconfig: context ${KUBE_CONTEXT}"
aws eks update-kubeconfig --region "${REGION}" --name "$(tfout cluster_name)" --alias "${KUBE_CONTEXT}" >/dev/null
kc get nodes -L topology.kubernetes.io/zone,node.kubernetes.io/instance-type,eks.amazonaws.com/capacityType

log "Account-specific values"
bash "${LOCAL_DIR}/scripts/aws-values.sh" "${ENV_NAME}"

log "Images"
bash "${LOCAL_DIR}/scripts/aws-images.sh" "${ENV_NAME}"

log "StorageClass"
kc apply -f "${REPO_ROOT}/gitops/bootstrap/storageclass-gp3.yaml"
# EKS creates gp2 as a default in some versions; only one default may exist.
kc annotate storageclass gp2 storageclass.kubernetes.io/is-default-class=false --overwrite >/dev/null 2>&1 || true
# Credentials are synced by External Secrets; nothing secret passes through this script.

bash "${LOCAL_DIR}/scripts/gitops-bootstrap.sh"
