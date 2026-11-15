#!/usr/bin/env bash
# Re-lock the ops agent's Python dependencies with hashes, from releases at least 14 days old.
#   usage: ai-lock.sh [cutoff YYYY-MM-DD] [uv options]     e.g. ai-lock.sh --upgrade-package oauthlib
# Existing pins are kept unless upgraded: a CVE fix needs --upgrade-package <name> (or --upgrade for all).
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require uv
# uv excludes uploads on or after the cutoff date, so the cutoff is the day after "14 days ago".
CUTOFF="$(date -u -d '13 days ago' +%Y-%m-%d)"
if [[ "${1:-}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then CUTOFF="$1"; shift; fi
for app in mcp-platform-tools ai-ops hermes; do
  args=(--python-version 3.14)
  [ "${app}" = hermes ] && args=(--python-version 3.13 --override overrides.txt)   # Hermes supports 3.11-3.13
  (cd "${REPO_ROOT}/apps/${app}" && uv pip compile requirements.in --universal --generate-hashes \
     --exclude-newer "${CUTOFF}" "${args[@]}" "$@" -q -o requirements.txt)
  ok "${app}: $(grep -c '==' "${REPO_ROOT}/apps/${app}/requirements.txt") packages, released before ${CUTOFF}"
done
