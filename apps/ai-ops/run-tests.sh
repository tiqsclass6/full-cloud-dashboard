#!/usr/bin/env bash
# Unit tests in the image's test stage, with a throwaway Postgres for the PostgresStore tests.
#   usage: ./run-tests.sh [pytest args]
set -euo pipefail
export MSYS_NO_PATHCONV=1      # Git Bash on Windows: keep container paths as written
cd "$(dirname "${BASH_SOURCE[0]}")"
docker build -q --target test -t lakehouse/ai-ops:test . >/dev/null

NET="ai-ops-test-$$"; PG="ai-ops-test-pg-$$"
cleanup() { docker rm -f "${PG}" >/dev/null 2>&1 || true; docker network rm "${NET}" >/dev/null 2>&1 || true; }
trap cleanup EXIT
docker network create "${NET}" >/dev/null
docker run -d --name "${PG}" --network "${NET}" -e POSTGRES_PASSWORD=test -e POSTGRES_DB=ops postgres:17-alpine >/dev/null
for _ in $(seq 1 30); do docker exec "${PG}" pg_isready -U postgres -d ops >/dev/null 2>&1 && break; sleep 1; done

docker run --rm --network "${NET}" -e OPS_TEST_DATABASE_URL="postgresql://postgres:test@${PG}:5432/ops" \
  lakehouse/ai-ops:test python -m pytest "$@"
