#!/usr/bin/env bash
set -euo pipefail

VENV_DIR=".venv-security"

python3 -m venv "${VENV_DIR}"
source "${VENV_DIR}/bin/activate"
python -m pip install --upgrade pip >/dev/null
python -m pip install -e . >/dev/null

echo "== Dependency integrity check (isolated venv) =="
python -m pip check

echo "== Vulnerability scan (pip-audit) =="
if python -m pip_audit --version >/dev/null 2>&1; then
  python -m pip_audit
  echo "PASS: pip-audit completed"
else
  echo "BLOCKED: pip-audit unavailable in this network-restricted environment."
  echo "Run this on CI/runner with internet access:"
  echo "  python -m pip install pip-audit && python -m pip_audit"
  exit 2
fi
