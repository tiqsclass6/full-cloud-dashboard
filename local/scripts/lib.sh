#!/usr/bin/env bash
# Shared settings and helpers for the local kind platform scripts.
set -euo pipefail

# Git Bash on Windows rewrites arguments that look like POSIX paths; turn that off.
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

# `pwd -W` yields C:/... on Git Bash for Windows, which native kubectl/docker/helm understand; plain `pwd` elsewhere.
LOCAL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && { pwd -W 2>/dev/null || pwd; })"
CLUSTER_NAME="lakehouse"
# The aws-*.sh scripts set GITOPS_ENV and KUBE_CONTEXT before sourcing this; everything else defaults to kind.
GITOPS_ENV="${GITOPS_ENV:-local}"
KUBE_CONTEXT="${KUBE_CONTEXT:-kind-${CLUSTER_NAME}}"

# Pinned versions (verified against the chart repos / registries when written).
KPS_CHART_VERSION="92.14.1"        # kube-prometheus-stack
STRIMZI_CHART_VERSION="1.3.0"      # Strimzi operator (Kafka 4.3.x, KRaft only)
CURL_IMAGE="curlimages/curl:8.11.1"
KAFKA_TOOLS_IMAGE="quay.io/strimzi/kafka:1.3.0-kafka-4.3.1"
FLINK_OPERATOR_CHART_VERSION="1.16.1"
FLINK_OPERATOR_IMAGE="apache/flink-kubernetes-operator:1.16.1"
FLINK_IMAGE="flink:2.1.3-java17"
SPARK_OPERATOR_CHART_VERSION="2.5.2"
SPARK_OPERATOR_IMAGE="ghcr.io/kubeflow/spark-operator/controller:2.5.2"
SPARK_IMAGE="apache/spark:4.1.4-scala2.13-java21-python3-ubuntu"
ARGOCD_CHART_VERSION="10.11.6"     # Argo CD v3.5.6
GITEA_IMAGE="gitea/gitea:28.2.0-rootless"
# x86 for kind and the EKS nodes (AL2023_x86_64); only the AgentCore agent image is arm64 (aws-images.sh).
IMAGE_PLATFORM="${IMAGE_PLATFORM:-linux/amd64}"

# Locally a throwaway Gitea password; on EKS a random one in the gitops/gitea-admin Secret.
REPO_ROOT="$(cd "${LOCAL_DIR}/.." && { pwd -W 2>/dev/null || pwd; })"
GITOPS_APP="platform-${GITOPS_ENV}"
GITEA_USER="lakehouse"
GITEA_REPO="lakehouse/platform"

# Windows jq ends lines with CRLF and bash keeps the CR ("0" + CR is not "0"): strip it everywhere.
jq() { command jq "$@" | tr -d '\r'; }

kc() { kubectl --context "${KUBE_CONTEXT}" "$@"; }
hc() { helm --kube-context "${KUBE_CONTEXT}" "$@"; }

is_kind() { [[ "${KUBE_CONTEXT}" == kind-* ]]; }

# COSIGN, else cosign on PATH, else the WinGet folder (Git Bash cannot follow WinGet's alias symlink).
cosign_bin() {
  local c="${COSIGN:-}"
  [ -z "${c}" ] && c="$(command -v cosign 2>/dev/null || true)"
  if [ -z "${c}" ]; then
    for f in "${LOCALAPPDATA:-/nonexistent}"/Microsoft/WinGet/Packages/Sigstore.Cosign*/cosign-windows-amd64.exe; do
      [ -f "${f}" ] && c="${f}" && break
    done
  fi
  [ -n "${c}" ] || { echo "cosign not found (install Sigstore.Cosign, or set COSIGN)" >&2; return 1; }
  echo "${c}"
}

# Gitea admin password: fixed locally; on EKS read from (or, first time, generated into) a Secret.
gitea_password() {
  if is_kind; then echo "lakehouse-local-gitops"; return; fi
  local pw
  pw="$(kc -n gitops get secret gitea-admin -o jsonpath='{.data.password}' 2>/dev/null | base64 -d || true)"
  if [ -z "${pw}" ]; then
    pw="$(openssl rand -hex 20)"
    kc create namespace gitops --dry-run=client -o yaml | kc apply -f - >/dev/null
    kc -n gitops create secret generic gitea-admin --from-literal=password="${pw}" >/dev/null
  fi
  echo "${pw}"
}

# localhost tries IPv4 and IPv6, and a stale forward may listen on only one (netstat is unreliable here).
port_in_use() { (exec 3<>"/dev/tcp/localhost/$1") 2>/dev/null; }
# Best effort: show who listens on the port (PID and command line), so the user can stop it.
port_holder() {
  if command -v powershell >/dev/null 2>&1; then
    powershell -NoProfile -Command "Get-NetTCPConnection -LocalPort $1 -State Listen -ErrorAction SilentlyContinue | ForEach-Object { \$p = Get-CimInstance Win32_Process -Filter ('ProcessId=' + \$_.OwningProcess); '  PID ' + \$_.OwningProcess + ': ' + \$p.CommandLine }" 2>/dev/null | tr -d '\r' || true
    echo "  stop it with: taskkill //PID <pid> //F"
  else
    { ss -ltnp 2>/dev/null || lsof -nP -iTCP:"$1" -sTCP:LISTEN 2>/dev/null; } | grep -E "[:.]$1[[:space:]]|:$1\b" || true
  fi
  return 0
}

# Command lines of running kubectl processes (Git Bash's ps cannot see Windows-native processes).
kubectl_cmdlines() {
  if command -v powershell >/dev/null 2>&1; then
    powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name='kubectl.exe'\" | ForEach-Object { \$_.CommandLine }" 2>/dev/null | tr -d '\r'
  else
    ps -eo args 2>/dev/null | grep '[k]ubectl' || true
  fi
}

# fwd <namespace> <svc/name> <local:remote>: background port-forward; FWD_PID is the real PID, so kill works on Windows.
fwd() {
  local port="${3%%:*}" tries
  # On a taken port the script would talk to whatever listens there; a just-stopped forward needs a moment.
  for tries in 1 2 3 4 5 6; do port_in_use "${port}" || break; sleep 0.5; done
  # The identical forward (e.g. from `make local-ports`) is reused and left running.
  if port_in_use "${port}" && kubectl_cmdlines | awk -v s="--context ${KUBE_CONTEXT} -n $1 port-forward $2 $3" \
       'substr($0, length($0) - length(s) + 1) == s { found = 1 } END { exit !found }'; then
    ok "reusing the running port-forward $1/$2 on localhost:${port}"
    FWD_PID=""
    return 0
  fi
  if port_in_use "${port}"; then
    echo "port-forward ${KUBE_CONTEXT}/$1/$2 needs localhost:${port}, but it is already in use:" >&2
    port_holder "${port}" >&2
    echo "Stop that process (a forward to another service or cluster?) and run this again." >&2
    exit 1
  fi
  local flog="${REPO_ROOT}/build/port-forward-${port}.log"
  mkdir -p "${REPO_ROOT}/build"
  kubectl --context "${KUBE_CONTEXT}" -n "$1" port-forward "$2" "$3" >"${flog}" 2>&1 &
  FWD_PID=$!
  PORT_FORWARD_PIDS+=("${FWD_PID}")
  # A forward that cannot start exits quickly (reserved port, no service, no access): report why now.
  for tries in $(seq 1 20); do                       # up to 10 s: listening, or the process is gone
    port_in_use "${port}" && break
    kill -0 "${FWD_PID}" 2>/dev/null || break
    sleep 0.5
  done
  if ! port_in_use "${port}"; then
    echo "port-forward ${KUBE_CONTEXT}/$1/$2 on localhost:${port} failed:" >&2
    sed 's/^/  /' "${flog}" >&2
    grep -q "forbidden by its access permissions" "${flog}" \
      && echo "  Windows has reserved this port: see docs/lab/RUNBOOK.md, Troubleshooting (netsh interface ipv4 show excludedportrange protocol=tcp)" >&2
    exit 1
  fi
}
PORT_FORWARD_PIDS=()
stop_forwards() { kill "${PORT_FORWARD_PIDS[@]}" 2>/dev/null || true; PORT_FORWARD_PIDS=(); }

log()  { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }
ok()   { printf '\033[1;32m  ok\033[0m  %s\n' "$*"; }
fail() { printf '\033[1;31m FAIL\033[0m %s\n' "$*"; FAILED=1; }

require() {
  for bin in "$@"; do
    command -v "$bin" >/dev/null 2>&1 || { echo "missing required tool: $bin" >&2; exit 1; }
  done
}

# docker_build [docker build args...]: for IMAGE_PLATFORM; DOCKER_BUILD_FLAGS="--pull --no-cache" forces a clean build.
docker_build() {
  # shellcheck disable=SC2086  # flags are split on purpose
  docker build -q --platform "${IMAGE_PLATFORM}" ${DOCKER_BUILD_FLAGS:-} "$@"
}

# preload_image <image> [force]: pull once on the host and stream to every node (per-node pulls stall; kind load fails on multi-arch).
preload_image() {
  local image="$1" force="${2:-}"
  is_kind || return 0    # EKS nodes pull from the registries themselves
  docker image inspect "$image" >/dev/null 2>&1 || docker pull --platform "${IMAGE_PLATFORM}" "$image"
  for node in $(kind get nodes --name "${CLUSTER_NAME}"); do
    if [ "$force" != "force" ] && docker exec "$node" crictl inspecti "$image" >/dev/null 2>&1; then
      ok "$image already on $node"
    else
      docker exec "$node" crictl rmi "$image" >/dev/null 2>&1 || true
      docker save "$image" | docker exec -i "$node" ctr --namespace=k8s.io images import - >/dev/null
      ok "$image loaded on $node"
    fi
  done
}

# Tests that change live resources on purpose pause Argo CD's automated sync, then resume it.

gitops_active() { kc -n argocd get application "${1:-${GITOPS_APP}}" >/dev/null 2>&1; }

gitops_pause() {  # gitops_pause [application]
  local app="${1:-${GITOPS_APP}}"
  gitops_active "${app}" || return 0
  kc -n argocd patch application "${app}" --type json \
    -p '[{"op":"remove","path":"/spec/syncPolicy/automated"}]' >/dev/null 2>&1 || true
  echo "GitOps: automated sync paused for ${app}"
}

gitops_resume() {  # gitops_resume [application]
  local app="${1:-${GITOPS_APP}}"
  gitops_active "${app}" || return 0
  kc -n argocd patch application "${app}" --type merge \
    -p '{"spec":{"syncPolicy":{"automated":{"prune":true,"selfHeal":true}}}}' >/dev/null
  echo "GitOps: automated sync resumed for ${app} (Argo CD restores Git's state)"
}

# Wait until an Argo CD application reports Synced and Healthy: gitops_wait <application> [timeout s]
gitops_wait() {
  local app="$1" timeout="${2:-600}" sync health
  for _ in $(seq 1 $((timeout / 5))); do
    sync="$(kc -n argocd get application "${app}" -o jsonpath='{.status.sync.status}' 2>/dev/null || true)"
    health="$(kc -n argocd get application "${app}" -o jsonpath='{.status.health.status}' 2>/dev/null || true)"
    [ "${sync}" = "Synced" ] && [ "${health}" = "Healthy" ] && { ok "${app}: Synced, Healthy"; return 0; }
    sleep 5
  done
  echo "timed out waiting for ${app} (sync=${sync:-?} health=${health:-?})" >&2
  return 1
}

# DESTRUCTIVE (local test data): delete job, group, topic and table for Argo CD to recreate; needs the 8181 port-forward.
reset_pipeline() {
  gitops_active || { echo "GitOps is not bootstrapped (make local-up)" >&2; return 1; }
  gitops_pause
  kc -n apps delete flinkdeployment events-to-iceberg --ignore-not-found --wait=true >/dev/null
  sleep 5
  kafka_cli kafka-consumer-groups.sh \
    --delete --group flink-events-to-iceberg >/dev/null 2>&1 || true
  kc -n kafka delete kafkatopic events.raw --ignore-not-found --wait=true >/dev/null
  for _ in $(seq 1 30); do   # the topic operator must remove the real topic before it is re-created
    kafka_cli kafka-topics.sh --list 2>/dev/null \
      | grep -qx events.raw || break
    sleep 2
  done
  curl -s -o /dev/null -X DELETE "http://localhost:8181/v1/namespaces/lakehouse/tables/events?purgeRequested=true" || true
  gitops_resume
  kc -n argocd annotate application "${GITOPS_APP}" argocd.argoproj.io/refresh=normal --overwrite >/dev/null
  for _ in $(seq 1 60); do
    [ "$(kc -n kafka get kafkatopic events.raw -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)" = "True" ] && break
    sleep 3
  done
  for _ in $(seq 1 100); do
    [ "$(kc -n apps get flinkdeployment events-to-iceberg -o jsonpath='{.status.jobStatus.state}' 2>/dev/null)" = "RUNNING" ] && break
    sleep 3
  done
  echo "pipeline reset: topic $(kc -n kafka get kafkatopic events.raw -o jsonpath='{.status.conditions[0].type}={.status.conditions[0].status}' 2>/dev/null)," \
       "flink job $(kc -n apps get flinkdeployment events-to-iceberg -o jsonpath='{.status.jobStatus.state}' 2>/dev/null)"
}

# Kafka CLI as kafka-admin from the admin toolbox: kafka_cli kafka-topics.sh --describe --topic events.raw
kafka_cli() {
  kc -n kafka exec -i deploy/kafka-admin -- kafka-cli "$@"
}

# curl from the in-cluster toolbox: in_cluster_curl [curl args...] (use fully qualified service names).
in_cluster_curl() {
  kc -n data exec deploy/toolbox -- curl -sS --max-time 20 "$@"
}
