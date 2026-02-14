from datetime import datetime, timedelta

from fastapi.testclient import TestClient

from data_autopilot.main import app
from data_autopilot.api import routes
from data_autopilot.db.session import SessionLocal


client = TestClient(app)


def test_alert_lifecycle_and_escalation() -> None:
    org = "org_alerts"
    headers = {"X-Tenant-Id": org, "X-User-Role": "member"}

    created = client.post(
        "/api/v1/alerts",
        headers=headers,
        json={
            "org_id": org,
            "dedupe_key": "dq.events.freshness",
            "title": "Events Freshness Delay",
            "message": "analytics.events delayed by 6 hours",
            "severity": "P1",
            "source_type": "data_quality",
            "source_id": "analytics.events",
        },
    )
    assert created.status_code == 200
    alert_id = created.json()["alert_id"]

    listed = client.get("/api/v1/alerts", params={"org_id": org}, headers=headers)
    assert listed.status_code == 200
    assert any(i["alert_id"] == alert_id for i in listed.json()["items"])

    ack = client.post(f"/api/v1/alerts/{alert_id}/ack", params={"org_id": org}, headers=headers, json={"user_id": "u_alert"})
    assert ack.status_code == 200
    assert ack.json()["status"] == "acknowledged"

    # Re-open by same dedupe key update; should stay non-resolved and same alert
    reopened = client.post(
        "/api/v1/alerts",
        headers=headers,
        json={
            "org_id": org,
            "dedupe_key": "dq.events.freshness",
            "title": "Events Freshness Delay",
            "message": "analytics.events delayed by 8 hours",
            "severity": "P1",
        },
    )
    assert reopened.status_code == 200
    assert reopened.json()["alert_id"] == alert_id

    # Force due escalation and process
    db = SessionLocal()
    try:
        rows = routes.alert_service.list_for_tenant(db, tenant_id=org)
        target = next(r for r in rows if r.id == alert_id)
        target.status = routes.AlertStatus.OPEN
        target.next_escalation_at = datetime.utcnow() - timedelta(minutes=1)
        db.add(target)
        db.commit()
    finally:
        db.close()

    escalated = client.post("/api/v1/alerts/escalate", params={"org_id": org}, headers=headers)
    assert escalated.status_code == 200
    assert alert_id in escalated.json()["alert_ids"]

    resolved = client.post(f"/api/v1/alerts/{alert_id}/resolve", params={"org_id": org}, headers=headers)
    assert resolved.status_code == 200
    assert resolved.json()["status"] == "resolved"
