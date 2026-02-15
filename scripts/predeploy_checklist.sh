#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${1:-deploy/.env.live}"

echo "== 1) Tests =="
python3 -m pytest -q

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "FAIL: env file not found: ${ENV_FILE}"
  exit 1
fi

while IFS= read -r line || [[ -n "$line" ]]; do
  line="${line#"${line%%[![:space:]]*}"}"
  [[ -z "${line}" || "${line:0:1}" == "#" ]] && continue
  key="${line%%=*}"
  value="${line#*=}"
  key="${key%"${key##*[![:space:]]}"}"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  if [[ "${value}" =~ ^\".*\"$ ]]; then
    value="${value:1:${#value}-2}"
  fi
  if [[ "${value}" =~ ^\'.*\'$ ]]; then
    value="${value:1:${#value}-2}"
  fi
  if [[ "${key}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    export "${key}=${value}"
  fi
done < "${ENV_FILE}"

echo "== 2) Config externalization checks =="
./scripts/live_readiness_check.sh "${ENV_FILE}"

echo "== 3) Smoke =="
if [[ "${BIGQUERY_MOCK_MODE:-true}" == "false" || "${METABASE_MOCK_MODE:-true}" == "false" ]]; then
  DATABASE_URL="sqlite+pysqlite:///./autopilot_smoke.db" python3 scripts/e2e_smoke.py --in-process --expect-live
else
  DATABASE_URL="sqlite+pysqlite:///./autopilot_smoke.db" python3 scripts/e2e_smoke.py --in-process
fi

echo "== 4) Error-handling + audit evidence =="
python3 -m pytest -q tests/integration/test_audit_payloads.py tests/integration/test_audit_events.py

echo "== 5) In-process performance check =="
python3 scripts/load_test_inprocess.py --requests 200 --concurrency 10

echo "== 6) Dependency + security checks =="
./scripts/security_scan.sh "${ENV_FILE}"

echo "== 7) Rollback artifacts =="
test -f deploy/ROLLBACK.md
test -x scripts/deploy_release.sh
test -x scripts/rollback_release.sh
echo "PASS: rollback docs/scripts present"

echo "Checklist run complete."
