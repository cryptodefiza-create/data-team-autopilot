#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"
ORG_ID="${ORG_ID:-org_ci}"
USER_ID="${USER_ID:-user_ci}"
PORT="${PORT:-8000}"
START_SERVER="${START_SERVER:-1}"

echo "== CI Verify: unit/integration tests =="
python3 -m pytest -q

echo "== CI Verify: migrations =="
python3 scripts/run_migrations.py

SERVER_PID=""
cleanup() {
  if [[ -n "${SERVER_PID}" ]]; then
    kill "${SERVER_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if [[ "${START_SERVER}" == "1" ]]; then
  echo "== CI Verify: starting local server =="
  PYTHONPATH=src python3 -m uvicorn data_autopilot.main:app --host 127.0.0.1 --port "${PORT}" >/tmp/data-autopilot-ci.log 2>&1 &
  SERVER_PID=$!

  echo "== CI Verify: waiting for health endpoint =="
  for _ in $(seq 1 40); do
    if curl -fsS "${BASE_URL}/health" >/dev/null 2>&1; then
      break
    fi
    sleep 0.25
  done
  curl -fsS "${BASE_URL}/health" >/dev/null
fi

echo "== CI Verify: smoke =="
python3 scripts/e2e_smoke.py --base-url "${BASE_URL}" --org-id "${ORG_ID}" --user-id "${USER_ID}"

echo "== CI Verify: load =="
python3 scripts/load_test.py --base-url "${BASE_URL}" --org-id "${ORG_ID}" --duration 10 --rps 2

echo "PASS: CI verify completed"
