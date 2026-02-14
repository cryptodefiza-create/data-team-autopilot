# Data Team Autopilot

Implementation covering phases 0-6 foundations with safety-first execution defaults.

## Included
- FastAPI app with health, agent run, and feedback endpoints
- Connector endpoints for BigQuery connect/disconnect lifecycle
- Core agent pipeline: planner -> validator -> critic -> executor -> composer
- SQL safety engine (`sqlglot`) blocking DDL/DML and multi-statement SQL
- Sliding-window cost limiter (Redis-backed with in-memory fallback)
- Cache layer (Redis-backed with in-memory fallback)
- BigQuery connector interface with dry-run and execution flow
- Metabase client integration (mock mode by default, live API supported)
- Dashboard generation with deterministic layout and idempotent versioning
- Weekly memo generation with automated validation and fallback
- Graceful degradation queue for memo/warehouse outages
- Workflow resiliency controls:
  - per-org concurrency caps with queueing (`concurrency_limit`)
  - resume on partial failures (workflow_id-aware)
  - dead-letter queue after repeated failed queue processing attempts
- Audit logging persistence
- Workflow persistence and idempotent step upsert
- Test suite scaffold (unit + integration)
- `/ready` endpoint for BigQuery + Metabase readiness checks
- Feedback analytics endpoint (`/api/v1/feedback/summary`)
- Artifact history endpoints:
  - `GET /api/v1/artifacts`
  - `GET /api/v1/artifacts/{artifact_id}`
  - `GET /api/v1/artifacts/{artifact_id}/versions`
  - `GET /api/v1/artifacts/{artifact_id}/lineage`
  - `GET /api/v1/artifacts/{artifact_id}/diff`
- `GET /api/v1/memos/{artifact_id}/wow`
- Workflow ops endpoints:
  - `GET /api/v1/workflows/queue`
  - `GET /api/v1/workflows/dead-letters`
  - `POST /api/v1/workflows/retry`
  - `GET /api/v1/workflows/runs`
  - `POST /api/v1/workflows/{workflow_id}/cancel`
- Tenant lifecycle endpoints:
  - `GET /api/v1/tenants/purge/preview`
  - `POST /api/v1/tenants/purge`
  - Purge requires admin + `confirm=true`
  - Purge blocks by default if active workflows exist (`force=true` overrides)
  - Audit log rows are retained by design
- Query approval endpoints:
  - `POST /api/v1/queries/preview`
  - `POST /api/v1/queries/approve-run`
- PII review endpoints:
  - `GET /api/v1/pii/review`
  - `POST /api/v1/pii/review/confirm`
- Alerting endpoints:
  - `POST /api/v1/alerts`
  - `GET /api/v1/alerts`
  - `POST /api/v1/alerts/{alert_id}/ack`
  - `POST /api/v1/alerts/{alert_id}/snooze`
  - `POST /api/v1/alerts/{alert_id}/resolve`
  - `POST /api/v1/alerts/escalate`
  - `GET /api/v1/alerts/policy`
  - `POST /api/v1/alerts/policy`
  - `GET /api/v1/alerts/routing`
  - `POST /api/v1/alerts/routing`
  - `GET /api/v1/alerts/notifications`
  - `POST /api/v1/alerts/reminders/process`
  - `POST /api/v1/alerts/notifications/retry`
  - `GET /api/v1/alerts/notifications/metrics`
  - Auto-generated alerts are emitted for:
    - workflow partial failures
    - dead-letter queue promotions
    - memo data-quality anomaly notes
- Connector disconnect behavior:
  - Marks connection disconnected and clears credentials
  - Cancels in-flight and queued workflows for that tenant
  - Marks tenant artifacts stale and purges connector cache
- RBAC + tenant boundary enforcement via headers:
  - `X-Tenant-Id`
  - `X-User-Role` (`admin`, `member`, `viewer`)

## Run
```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
uvicorn data_autopilot.main:app --reload
```

## Docker Staging (App + Redis + Metabase)
```bash
docker compose up --build -d
```

Endpoints:
- App health: `http://localhost:8000/health`
- App readiness: `http://localhost:8000/ready`
- Metabase: `http://localhost:3000`

Compose files:
- `docker-compose.yml`
- `deploy/.env.staging` (active staging env)
- `deploy/.env.staging.example` (template)

## Test
```bash
pytest
```

## Verification Scripts
- Migrations:
```bash
python scripts/run_migrations.py
```
- End-to-end smoke:
```bash
python scripts/e2e_smoke.py --base-url http://localhost:8000
# or run without network sockets (useful in restricted sandboxes):
python scripts/e2e_smoke.py --in-process
```
- Basic load test:
```bash
python scripts/load_test.py --base-url http://localhost:8000 --duration 30 --rps 2
```

- All-in-one CI verify (tests + migrations + smoke + load):
```bash
./scripts/ci_verify.sh
```

## Notes
- `ALLOW_REAL_QUERY_EXECUTION` defaults to `false`.
- Metabase runs in mock mode by default (`METABASE_MOCK_MODE=true`).
- BigQuery runs in mock mode by default (`BIGQUERY_MOCK_MODE=true`).
- Redis is optional in local dev; service automatically falls back to in-memory stores.

## Live Mode Checklist
Set the following for live deployment:
- `BIGQUERY_MOCK_MODE=false`
- `BIGQUERY_PROJECT_ID=<your_project>`
- `METABASE_MOCK_MODE=false`
- `METABASE_URL=<your_metabase_url>`
- `METABASE_API_KEY=<your_api_key>`
- `RUN_STARTUP_CONNECTION_TESTS=true`
- optionally set `ALLOW_REAL_QUERY_EXECUTION=true` only after readiness checks pass

When `ALLOW_REAL_QUERY_EXECUTION=true`, startup validation enforces:
- BigQuery mock mode is disabled
- live BigQuery project is configured
- if Metabase live mode is enabled, URL + API key are configured

## Docker Live Cutover
1. Copy `deploy/.env.live.example` to `deploy/.env.live` and fill required values.
2. Set `RUN_STARTUP_CONNECTION_TESTS=true`.
3. Run static readiness checks:
```bash
./scripts/live_readiness_check.sh deploy/.env.live
```
4. Restart services:
```bash
docker compose --env-file deploy/.env.live up --build -d
```
5. Verify:
```bash
curl -s http://localhost:8000/ready
```
