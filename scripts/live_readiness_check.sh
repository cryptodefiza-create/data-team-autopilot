#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${1:-deploy/.env.staging}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "FAIL: env file not found: $ENV_FILE"
  exit 1
fi

# shellcheck disable=SC2046
export $(grep -E '^[A-Za-z_][A-Za-z0-9_]*=' "$ENV_FILE" | xargs)

fail=0

check_required() {
  local key="$1"
  local val="${!key:-}"
  if [[ -z "$val" || "$val" == "CHANGE_ME" ]]; then
    echo "FAIL: $key is required and unset"
    fail=1
  else
    echo "OK: $key is set"
  fi
}

check_bool() {
  local key="$1"
  local expected="$2"
  local val="${!key:-}"
  if [[ "$val" != "$expected" ]]; then
    echo "FAIL: $key=$val (expected $expected)"
    fail=1
  else
    echo "OK: $key=$val"
  fi
}

echo "== Live Mode Static Checks =="

check_bool BIGQUERY_MOCK_MODE false
check_bool METABASE_MOCK_MODE false
check_bool RUN_STARTUP_CONNECTION_TESTS true
check_bool ALLOW_REAL_QUERY_EXECUTION false

check_required BIGQUERY_PROJECT_ID
check_required METABASE_URL
check_required METABASE_API_KEY
check_required REDIS_URL
check_required DATABASE_URL

if [[ $fail -eq 0 ]]; then
  echo "PASS: static live-mode env checks passed"
else
  echo "FAIL: static live-mode env checks failed"
fi

exit $fail
