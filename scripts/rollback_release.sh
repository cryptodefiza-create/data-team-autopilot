#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${1:-deploy/.env.live}"
STATE_DIR=".deploy"

if [[ ! -f "${STATE_DIR}/previous_release" ]]; then
  echo "FAIL: no previous release recorded"
  exit 1
fi

PREV="$(cat "${STATE_DIR}/previous_release")"
if [[ -z "${PREV}" ]]; then
  echo "FAIL: previous release is empty"
  exit 1
fi

echo "Rolling back to release ${PREV}"
git checkout "${PREV}"
docker compose --env-file "${ENV_FILE}" up --build -d
curl -fsS http://localhost:8000/health >/dev/null
echo "${PREV}" > "${STATE_DIR}/current_release"
echo "PASS: rollback to ${PREV} complete"
