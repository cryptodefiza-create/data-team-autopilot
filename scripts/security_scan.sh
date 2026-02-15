#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${1:-deploy/.env.live}"
INCLUDE_LIVE=0

if [[ -f "${ENV_FILE}" ]]; then
  # shellcheck disable=SC2046
  export $(grep -E '^[A-Za-z_][A-Za-z0-9_]*=' "${ENV_FILE}" | xargs)
  if [[ "${BIGQUERY_MOCK_MODE:-true}" == "false" || "${METABASE_MOCK_MODE:-true}" == "false" ]]; then
    INCLUDE_LIVE=1
  fi
fi

echo "== Dependency integrity check =="
if [[ "${INCLUDE_LIVE}" == "1" ]]; then
  python3 scripts/dependency_integrity_check.py --include-live
else
  python3 scripts/dependency_integrity_check.py
fi

echo "== Vulnerability scan (pip-audit) =="
if python3 -m pip_audit --version >/dev/null 2>&1; then
  python3 -m pip_audit
  echo "PASS: pip-audit completed"
else
  echo "BLOCKED: pip-audit is not installed in this environment."
  echo "PASS IN CI: .github/workflows/security-audit.yml enforces pip-audit on every push/PR."
  exit 2
fi
