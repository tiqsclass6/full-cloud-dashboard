#!/usr/bin/env bash
# Publish the working tree to the in-cluster Gitea through a separate Git dir (.gitops-mirror); this repo is untouched.
#   usage: gitops-push.sh ["message"]
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require git kubectl

MESSAGE="${1:-snapshot $(date -u +%Y-%m-%dT%H:%M:%SZ)}"
export GIT_DIR="${REPO_ROOT}/.gitops-mirror"
export GIT_WORK_TREE="${REPO_ROOT}"

if [ ! -d "${GIT_DIR}" ]; then
  git init -q -b main
fi
# Files are LF in the working tree; keep them byte-for-byte (and silence Git for Windows' CRLF warnings).
git config core.autocrlf false
git config core.safecrlf false

# Vendored chart dependencies must be current, because Argo CD only builds the top chart's.
for chart in "${REPO_ROOT}"/helm/charts/*/; do
  name="$(basename "${chart}")"
  [ "${name}" = "lakehouse-lib" ] && continue
  (cd "${chart}" && helm dependency build >/dev/null) || { echo "helm dependency build failed in ${name}" >&2; exit 1; }
done

git add -A
# Account-specific values are git-ignored here but must reach the cluster's Git.
if is_kind; then
  # The mirror is shared with EKS pushes: keep account-specific values out of the local Gitea.
  git rm -r -q --cached --ignore-unmatch helm/values/generated 'gitops/operators/*/generated-*.yaml' >/dev/null
else
  git add -f "helm/values/generated/${GITOPS_ENV}.yaml" gitops/operators/*/"generated-${GITOPS_ENV}.yaml"
fi
if git diff --cached --quiet && git rev-parse -q --verify HEAD >/dev/null; then
  echo "GitOps: no changes since the last snapshot ($(git rev-parse --short HEAD))"
else
  git -c user.name="gitops-snapshot" -c user.email="gitops@lakehouse.local" commit -q -m "${MESSAGE}"
  echo "GitOps: snapshot $(git rev-parse --short HEAD) - ${MESSAGE}"
fi

trap stop_forwards EXIT
fwd gitops svc/gitea-http 3300:3000
for _ in $(seq 1 20); do
  curl -s -o /dev/null "http://localhost:3300/api/healthz" && break
  sleep 1
done
git push -q --force "http://${GITEA_USER}:$(gitea_password)@localhost:3300/${GITEA_REPO}.git" HEAD:main
echo "GitOps: pushed to gitea ${GITEA_REPO} (main). Argo CD picks it up within about a minute;"
echo "        to sync now: kubectl -n argocd annotate application ${GITOPS_APP} argocd.argoproj.io/refresh=hard --overwrite"
