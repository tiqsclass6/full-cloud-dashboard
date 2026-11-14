#!/usr/bin/env bash
# promtool check and unit tests for the alert rules, from the cluster's Prometheus release. No cluster needed.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require docker

PROMTOOL_IMAGE="quay.io/prometheus/prometheus:v3.15.1"   # match kube-prometheus-stack's Prometheus
CHART="${REPO_ROOT}/helm/charts/observability"

promtool() {
  docker run --rm -v "${CHART}:/chart:ro" -w /chart/tests --entrypoint promtool "${PROMTOOL_IMAGE}" "$@"
}

log "promtool check rules"
promtool check rules $(cd "${CHART}" && ls rules/*.yaml | sed 's|^|/chart/|')

log "promtool test rules"
promtool test rules /chart/tests/alerts.test.yaml
