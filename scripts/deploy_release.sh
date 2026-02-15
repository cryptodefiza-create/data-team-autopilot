#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${1:-deploy/.env.live}"
RELEASE="${2:-$(git rev-parse --short HEAD)}"
STATE_DIR=".deploy"
mkdir -p "${STATE_DIR}"

PREV=""
if [[ -f "${STATE_DIR}/current_release" ]]; then
  PREV="$(cat "${STATE_DIR}/current_release")"
fi

echo "${PREV}" > "${STATE_DIR}/previous_release"
echo "${RELEASE}" > "${STATE_DIR}/current_release"

echo "Deploying release ${RELEASE} (previous: ${PREV:-none})"
docker compose --env-file "${ENV_FILE}" up --build -d
curl -fsS http://localhost:8000/health >/dev/null
echo "PASS: release ${RELEASE} deployed"
