#!/usr/bin/env bash
# Lint, template and kubeconform the charts for local, dev, staging and prod (KUBECONFORM overrides the binary).
#   usage: helm-check.sh [--server]   --server also dry-runs against the kind cluster
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require helm

HELM_DIR="${REPO_ROOT}/helm"
OUT="$(mktemp -d)"
trap 'rm -rf "${OUT}"' EXIT
KUBECONFORM="${KUBECONFORM:-$(command -v kubeconform || true)}"
FAILED=0

log "Chart dependencies"
for chart in "${HELM_DIR}"/charts/*/; do
  [ "$(basename "${chart}")" = "lakehouse-lib" ] && continue
  (cd "${chart}" && helm dependency build >/dev/null) && ok "$(basename "${chart}")" || fail "$(basename "${chart}")"
done

for env in local dev staging prod; do
  if [ "${env}" = local ]; then files=(-f "${HELM_DIR}/values/local.yaml")
  else files=(-f "${HELM_DIR}/values/aws.yaml" -f "${HELM_DIR}/values/${env}.yaml"); fi
  log "${env}"
  helm lint "${HELM_DIR}/charts/platform" "${files[@]}" --quiet >/dev/null 2>&1 && ok "lint" || fail "lint"
  if helm template platform "${HELM_DIR}/charts/platform" "${files[@]}" > "${OUT}/${env}.yaml" 2> "${OUT}/${env}.err"; then
    ok "template: $(grep -c '^kind:' "${OUT}/${env}.yaml") objects"
  else
    fail "template: $(head -3 "${OUT}/${env}.err")"; continue
  fi
  if [ -n "${KUBECONFORM}" ]; then
    # stdin, not a path: the Windows binary cannot open Git Bash style paths.
    if summary="$("${KUBECONFORM}" -strict -summary -kubernetes-version 1.33.0 -schema-location default \
         -schema-location 'https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json' \
         -output text - < "${OUT}/${env}.yaml" 2>&1)"; then
      ok "kubeconform: $(echo "${summary}" | tail -1)"
    else
      fail "kubeconform:"; echo "${summary}" | tail -8
    fi
  else
    echo "  (kubeconform not found: schema validation skipped; set KUBECONFORM=...)"
  fi
  if [ "${1:-}" = "--server" ]; then
    kc apply --dry-run=server -f - < "${OUT}/${env}.yaml" >/dev/null 2> "${OUT}/${env}.dry" \
      && ok "server-side dry run" || { fail "server-side dry run"; head -5 "${OUT}/${env}.dry"; }
  fi
done

echo
[ "${FAILED}" -eq 0 ] && printf '\033[1;32mAll chart checks passed.\033[0m\n' || { printf '\033[1;31mChart checks failed.\033[0m\n'; exit 1; }
