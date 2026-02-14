from fastapi.testclient import TestClient

from data_autopilot.api import routes
from data_autopilot.db.session import SessionLocal
from data_autopilot.main import app
from data_autopilot.models.entities import CatalogTable


client = TestClient(app)


def test_auto_alert_generated_from_workflow_partial_failure() -> None:
    org = "org_auto_alert_partial"
    headers = {"X-Tenant-Id": org, "X-User-Role": "member"}

    original = routes.workflow_service.run_profile_flow

    def fake_partial(db, tenant_id, payload=None, workflow_id=None):
        return {
            "workflow_id": "wf_fake_partial",
            "workflow_status": "partial_failure",
            "failed_step": {"step": "profile_columns", "error": "permission_denied", "retry_count": 3},
            "completed_steps": [],
            "available_actions": [],
        }

    routes.workflow_service.run_profile_flow = fake_partial  # type: ignore[assignment]
    try:
        r = client.post("/api/v1/workflows/profile", params={"org_id": org}, headers=headers)
        assert r.status_code == 200
        assert r.json()["workflow_status"] == "partial_failure"
    finally:
        routes.workflow_service.run_profile_flow = original  # type: ignore[assignment]

    alerts = client.get("/api/v1/alerts", params={"org_id": org}, headers=headers)
    assert alerts.status_code == 200
    items = alerts.json()["items"]
    assert any(i["title"].startswith("profile workflow partial failure") for i in items)


def test_auto_alert_generated_from_memo_anomaly() -> None:
    org = "org_auto_alert_memo"
    headers = {"X-Tenant-Id": org, "X-User-Role": "member"}

    prof = client.post("/api/v1/workflows/profile", params={"org_id": org}, headers=headers)
    assert prof.status_code == 200
    assert prof.json().get("status") == "success"

    db = SessionLocal()
    try:
        table = db.query(CatalogTable).filter(CatalogTable.tenant_id == org).first()
        assert table is not None
        table.freshness_hours = 8
        db.add(table)
        db.commit()
    finally:
        db.close()

    memo = client.post("/api/v1/workflows/memo", params={"org_id": org}, headers=headers)
    assert memo.status_code == 200
    assert memo.json().get("status") == "success"

    alerts = client.get("/api/v1/alerts", params={"org_id": org}, headers=headers)
    assert alerts.status_code == 200
    items = alerts.json()["items"]
    dq_items = [i for i in items if i["title"] == "Data quality anomaly detected"]
    assert len(dq_items) >= 1


def test_auto_alert_generated_on_dead_letter() -> None:
    org = "org_auto_alert_dlq"
    headers = {"X-Tenant-Id": org, "X-User-Role": "member"}

    db = SessionLocal()
    try:
        routes.degradation_service.enqueue(
            db,
            tenant_id=org,
            workflow_type="memo",
            payload={"org_id": org, "failure_modes": {"store_artifact": {"mode": "permission_denied", "remaining": 10}}},
            reason="llm_unavailable",
        )
    finally:
        db.close()

    for _ in range(3):
        processed = client.post("/api/v1/workflows/process-queue", params={"org_id": org}, headers=headers)
        assert processed.status_code == 200

    alerts = client.get("/api/v1/alerts", params={"org_id": org}, headers=headers)
    assert alerts.status_code == 200
    items = alerts.json()["items"]
    assert any(i["severity"] == "P0" and "dead letter" in i["title"] for i in items)
