#!/usr/bin/env bash
# Draw light reference diagrams into build/diagrams; docs/diagrams (the dark set) is never touched.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require uv dot

TOOLS="${LOCAL_DIR}/tools/diagrams"
VENV="${REPO_ROOT}/build/diagrams-venv"
OUT="${REPO_ROOT}/build/diagrams"
PY="${VENV}/Scripts/python.exe"; [ -x "${PY}" ] || PY="${VENV}/bin/python"

if [ ! -x "${PY}" ]; then
  log "Creating ${VENV}"
  uv venv -q --python 3.13 "${VENV}"
  PY="${VENV}/Scripts/python.exe"; [ -x "${PY}" ] || PY="${VENV}/bin/python"
fi
uv pip install -q --python "${PY}" --require-hashes -r "${TOOLS}/requirements.txt"

mkdir -p "${OUT}"
log "Architecture diagrams"
"${PY}" "${TOOLS}/architecture.py" "${OUT}"
log "Workflow infographics"
"${PY}" "${TOOLS}/workflows.py" "${OUT}"
ok "light reference diagrams in build/diagrams (docs/diagrams unchanged)"
