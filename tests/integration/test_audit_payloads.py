from __future__ import annotations

from uuid import uuid4

from fastapi.testclient import TestClient

from data_autopilot.db.session import SessionLocal
from data_autopilot.main import app
from data_autopilot.models.entities import AuditLog


client = TestClient(app)


def _latest_event(org: str, event_type: str) -> AuditLog:
    db = SessionLocal()
    try:
        row = (
            db.query(AuditLog)
            .filter(AuditLog.tenant_id == org, AuditLog.event_type == event_type)
            .order_by(AuditLog.created_at.desc(), AuditLog.id.desc())
            .first()
        )
        assert row is not None, f"missing audit event: {event_type}"
        return row
    finally:
        db.close()


def test_http_exception_audit_for_invalid_query_preview_payload() -> None:
    org = f"org_audit_bad_preview_{uuid4().hex[:8]}"
    headers = {"X-Tenant-Id": org, "X-User-Role": "member"}

    resp = client.post("/api/v1/queries/preview", headers=headers, json={"org_id": org, "sql": ""})
    assert resp.status_code == 400

    event = _latest_event(org, "http_exception")
    payload = event.payload
    assert payload["status_code"] == 400
    assert "sql is required" in payload["detail"]
    assert payload["method"] == "POST"
    assert payload["path"] == "/api/v1/queries/preview"
    assert payload["role"] == "member"


def test_http_exception_audit_for_rbac_denial_payload() -> None:
    org = f"org_audit_rbac_{uuid4().hex[:8]}"
    headers = {"X-Tenant-Id": org, "X-User-Role": "viewer"}

    resp = client.post("/api/v1/workflows/profile", params={"org_id": org}, headers=headers)
    assert resp.status_code == 403

    event = _latest_event(org, "http_exception")
    payload = event.payload
    assert payload["status_code"] == 403
    assert "Insufficient role" in payload["detail"]
    assert payload["method"] == "POST"
    assert payload["path"] == "/api/v1/workflows/profile"
    assert payload["role"] == "viewer"


def test_query_preview_audit_payload_for_blocked_sql() -> None:
    org = f"org_audit_blocked_sql_{uuid4().hex[:8]}"
    headers = {"X-Tenant-Id": org, "X-User-Role": "member"}

    resp = client.post("/api/v1/queries/preview", headers=headers, json={"org_id": org, "sql": "DROP TABLE users"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "blocked"

    event = _latest_event(org, "query_previewed")
    payload = event.payload
    assert payload["status"] == "blocked"
    assert payload["requires_approval"] is False


def test_workflow_run_audit_payload_includes_artifact_id_on_success() -> None:
    org = f"org_audit_workflow_success_{uuid4().hex[:8]}"
    headers = {"X-Tenant-Id": org, "X-User-Role": "member"}

    profile = client.post("/api/v1/workflows/profile", params={"org_id": org}, headers=headers)
    assert profile.status_code == 200
    assert profile.json()["status"] == "success"

    dash = client.post("/api/v1/workflows/dashboard", params={"org_id": org}, headers=headers)
    assert dash.status_code == 200
    assert dash.json()["status"] == "success"
    artifact_id = dash.json()["artifact_id"]

    event = _latest_event(org, "workflow_run")
    payload = event.payload
    assert payload["workflow_type"] == "dashboard"
    assert payload["status"] == "success"
    assert payload["artifact_id"] == artifact_id


def test_tenant_purge_preview_audit_payload_has_counts() -> None:
    org = f"org_audit_purge_preview_{uuid4().hex[:8]}"
    headers = {"X-Tenant-Id": org, "X-User-Role": "admin"}

    preview = client.get("/api/v1/tenants/purge/preview", params={"org_id": org}, headers=headers)
    assert preview.status_code == 200
    body = preview.json()
    assert "counts" in body

    event = _latest_event(org, "tenant_purge_previewed")
    payload = event.payload
    assert payload["org_id"] == org
    assert isinstance(payload["counts"], dict)
    assert "tenant_rows" in payload["counts"]
