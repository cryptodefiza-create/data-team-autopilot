from uuid import uuid4
from datetime import datetime

from fastapi.testclient import TestClient

from data_autopilot.db.session import SessionLocal
from data_autopilot.main import app
from data_autopilot.models.entities import (
    AlertEvent,
    AlertNotification,
    AlertNotificationStatus,
    AlertSeverity,
    AlertStatus,
    Artifact,
    ArtifactType,
    AuditLog,
    Connection,
    Feedback,
    FeedbackType,
    QueryApproval,
    Role,
    Tenant,
    User,
    WorkflowRun,
)


client = TestClient(app)


def test_tenant_purge_preview_and_execute_retains_audit() -> None:
    org = f"org_purge_{uuid4().hex[:8]}"
    headers = {"X-Tenant-Id": org, "X-User-Role": "admin"}

    db = SessionLocal()
    try:
        db.add(Tenant(id=org, name=org, settings={}))
        db.add(User(id=f"user_{org}", tenant_id=org, email=f"{org}@example.com", role=Role.ADMIN))
        db.add(Connection(id=f"conn_{org}", tenant_id=org, status="active", config_encrypted={"k": "v"}))
        db.add(Artifact(id=f"art_{org}", tenant_id=org, type=ArtifactType.DASHBOARD, version=1, data={"ok": True}))
        db.add(
            Feedback(
                id=f"fb_{org}",
                tenant_id=org,
                user_id=f"user_{org}",
                artifact_id=f"art_{org}",
                artifact_version=1,
                artifact_type=ArtifactType.DASHBOARD,
                feedback_type=FeedbackType.POSITIVE,
            )
        )
        db.add(QueryApproval(id=f"qa_{org}", tenant_id=org, sql="SELECT 1", status="approved"))
        alert_id = f"al_{org}"
        db.add(
            AlertEvent(
                id=alert_id,
                tenant_id=org,
                dedupe_key=f"dedupe_{org}",
                title="t",
                message="m",
                severity=AlertSeverity.P2,
                status=AlertStatus.OPEN,
                next_escalation_at=datetime.utcnow(),
            )
        )
        db.add(
            AlertNotification(
                id=f"an_{org}",
                tenant_id=org,
                alert_id=alert_id,
                severity=AlertSeverity.P2,
                event_type="created",
                channel_type="email",
                channel_target="ops@example.com",
                recipient="ops@example.com",
                status=AlertNotificationStatus.QUEUED,
            )
        )
        db.add(AuditLog(tenant_id=org, event_type="seed", payload={"ok": True}))
        db.commit()
    finally:
        db.close()

    preview = client.get("/api/v1/tenants/purge/preview", params={"org_id": org}, headers=headers)
    assert preview.status_code == 200
    body = preview.json()
    assert body["tenant_exists"] is True
    assert body["counts"]["tenant_rows"] == 1
    assert body["counts"]["connections"] == 1
    assert body["audit_rows_retained"] >= 1

    purged = client.post("/api/v1/tenants/purge", headers=headers, json={"org_id": org, "confirm": True, "force": True})
    assert purged.status_code == 200
    out = purged.json()
    assert out["status"] == "purged"
    assert out["deleted"]["tenant_rows"] == 1
    assert out["deleted"]["connections"] == 1
    assert out["audit_rows_retained"] >= 2

    db = SessionLocal()
    try:
        assert db.query(Tenant).filter(Tenant.id == org).count() == 0
        assert db.query(Connection).filter(Connection.tenant_id == org).count() == 0
        # Audit rows are retained by design.
        assert db.query(AuditLog).filter(AuditLog.tenant_id == org).count() >= 2
    finally:
        db.close()


def test_tenant_purge_blocks_on_active_workflow_without_force() -> None:
    org = f"org_purge_block_{uuid4().hex[:8]}"
    headers = {"X-Tenant-Id": org, "X-User-Role": "admin"}

    db = SessionLocal()
    try:
        db.add(Tenant(id=org, name=org, settings={}))
        db.add(WorkflowRun(id=f"wf_{org}", tenant_id=org, workflow_type="profile", status="running"))
        db.commit()
    finally:
        db.close()

    blocked = client.post("/api/v1/tenants/purge", headers=headers, json={"org_id": org, "confirm": True})
    assert blocked.status_code == 409
    assert blocked.json()["detail"]["status"] == "blocked_active_workflows"
