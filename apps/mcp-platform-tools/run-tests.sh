#!/usr/bin/env bash
# Runs the unit tests inside the image's test stage (Linux, the locked packages, no cluster needed).
#   usage: ./run-tests.sh [pytest args]
set -euo pipefail
export MSYS_NO_PATHCONV=1      # Git Bash on Windows: keep container paths as written
cd "$(dirname "${BASH_SOURCE[0]}")"
docker build -q --target test --build-context runbooks=../../docs/runbooks -t lakehouse/mcp-platform-tools:test . >/dev/null
docker run --rm -e RUNBOOK_DIR=/opt/runbooks lakehouse/mcp-platform-tools:test python -m pytest "$@"
