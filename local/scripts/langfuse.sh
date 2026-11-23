#!/usr/bin/env bash
# Turn Langfuse (the ops agent's LLM tracing, kind only) on or off. Off by default: it uses about 2 GiB and
# receives nothing until the agent has model access.
#   usage: langfuse.sh on|off|status
# The switch is the label lakehouse.io/langfuse=on on Argo CD's cluster Secret, which the langfuse ApplicationSet
# selects on. Off deletes the Application (and with it the pods); ClickHouse's volume and the langfuse database
# stay, so turning it on again keeps earlier traces.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require kubectl
is_kind || { echo "Langfuse runs only on kind" >&2; exit 2; }
CLUSTER_SECRET="cluster-${GITOPS_ENV}"
APP="langfuse-${GITOPS_ENV}"

restart_agent() {  # the agent reads the Langfuse keys at start-up
  kc -n ai-ops get deploy ops-agent >/dev/null 2>&1 || return 0
  kc -n ai-ops delete pod -l app=ops-agent --wait=false >/dev/null && ok "ops-agent restarted"
}

case "${1:-}" in
  on)
    log "Secrets and database"
    bash "${LOCAL_DIR}/scripts/deploy-langfuse.sh" >/dev/null && ok "Langfuse secrets and database present"
    log "Enable"
    kc -n argocd label secret "${CLUSTER_SECRET}" lakehouse.io/langfuse=on --overwrite >/dev/null
    for _ in $(seq 1 24); do gitops_active "${APP}" && break; sleep 5; done
    gitops_wait "${APP}" 900
    restart_agent
    echo "UI: make local-ports, then http://localhost:3001"
    ;;
  off)
    log "Disable"
    kc -n argocd label secret "${CLUSTER_SECRET}" lakehouse.io/langfuse- >/dev/null 2>&1 || true
    for _ in $(seq 1 60); do gitops_active "${APP}" || break; sleep 5; done
    gitops_active "${APP}" && { echo "${APP} still present after 5 minutes" >&2; exit 1; }
    ok "${APP} deleted"
    for _ in $(seq 1 60); do
      [ -z "$(kc -n langfuse get pods --no-headers 2>/dev/null)" ] && break; sleep 5
    done
    ok "langfuse pods gone (volume and database kept)"
    kc -n ai-ops delete secret langfuse-api --ignore-not-found >/dev/null && restart_agent
    ;;
  status)
    if [ "$(kc -n argocd get secret "${CLUSTER_SECRET}" -o jsonpath='{.metadata.labels.lakehouse\.io/langfuse}')" = "on" ]; then
      echo "on"; kc -n langfuse get pods 2>/dev/null || true
    else
      echo "off"
    fi
    ;;
  *) sed -n 2,4p "$0" >&2; exit 2 ;;
esac
