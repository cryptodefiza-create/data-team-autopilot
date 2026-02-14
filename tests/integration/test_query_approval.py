from fastapi.testclient import TestClient

from data_autopilot.api import routes
from data_autopilot.main import app


client = TestClient(app)


def test_query_preview_requires_approval_then_executes() -> None:
    org = "org_query_approval"
    headers = {"X-Tenant-Id": org, "X-User-Role": "member"}

    old_soft = routes.query_service.settings.per_query_max_bytes
    old_hard = routes.query_service.settings.per_query_max_bytes_with_approval
    try:
        routes.query_service.settings.per_query_max_bytes = 1
        routes.query_service.settings.per_query_max_bytes_with_approval = 10_000_000

        preview = client.post(
            "/api/v1/queries/preview",
            headers=headers,
            json={"org_id": org, "sql": "SELECT DATE(created_at) AS day, COUNT(DISTINCT user_id) AS dau FROM analytics.events GROUP BY 1"},
        )
        assert preview.status_code == 200
        body = preview.json()
        assert body["status"] == "approval_required"
        assert body["requires_approval"] is True

        run = client.post(
            "/api/v1/queries/approve-run",
            headers=headers,
            json={"org_id": org, "preview_id": body["preview_id"]},
        )
        assert run.status_code == 200
        run_body = run.json()
        assert run_body["status"] == "executed"
        assert len(run_body["rows"]) >= 1
    finally:
        routes.query_service.settings.per_query_max_bytes = old_soft
        routes.query_service.settings.per_query_max_bytes_with_approval = old_hard


def test_query_preview_hard_blocks() -> None:
    org = "org_query_hard_block"
    headers = {"X-Tenant-Id": org, "X-User-Role": "member"}
    old_hard = routes.query_service.settings.per_query_max_bytes_with_approval
    try:
        routes.query_service.settings.per_query_max_bytes_with_approval = 1
        preview = client.post(
            "/api/v1/queries/preview",
            headers=headers,
            json={"org_id": org, "sql": "SELECT DATE(created_at) AS day, COUNT(DISTINCT user_id) AS dau FROM analytics.events GROUP BY 1"},
        )
        assert preview.status_code == 200
        body = preview.json()
        assert body["status"] == "blocked"
        assert "hard max" in body["reasons"][0]
    finally:
        routes.query_service.settings.per_query_max_bytes_with_approval = old_hard
