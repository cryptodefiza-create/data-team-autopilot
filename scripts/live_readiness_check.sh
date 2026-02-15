#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${1:-deploy/.env.staging}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "FAIL: env file not found: $ENV_FILE"
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
