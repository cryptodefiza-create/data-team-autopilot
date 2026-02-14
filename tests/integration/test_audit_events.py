from fastapi.testclient import TestClient

from data_autopilot.db.session import SessionLocal
from data_autopilot.main import app
from data_autopilot.models.entities import AuditLog


client = TestClient(app)


def test_audit_events_written_for_success_and_denial() -> None:
    org = "org_audit_events"
    admin_headers = {"X-Tenant-Id": org, "X-User-Role": "admin"}
    member_headers = {"X-Tenant-Id": org, "X-User-Role": "member"}
    viewer_headers = {"X-Tenant-Id": org, "X-User-Role": "viewer"}

    connect = client.post(
        "/api/v1/connectors/bigquery",
        json={"org_id": org, "service_account_json": {"client_email": "audit@example.com"}},
        headers=admin_headers,
    )
    assert connect.status_code == 200

    profile = client.post("/api/v1/workflows/profile", params={"org_id": org}, headers=member_headers)
    assert profile.status_code == 200

    denied = client.post("/api/v1/workflows/profile", params={"org_id": org}, headers=viewer_headers)
    assert denied.status_code == 403

    db = SessionLocal()
    try:
        rows = (
            db.query(AuditLog)
            .filter(AuditLog.tenant_id == org)
            .order_by(AuditLog.created_at.desc())
            .all()
        )
        event_types = {r.event_type for r in rows}
        assert "connector_connected" in event_types
        assert "workflow_run" in event_types
        assert "http_exception" in event_types
    finally:
        db.close()
