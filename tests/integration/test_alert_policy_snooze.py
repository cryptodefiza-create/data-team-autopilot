from datetime import datetime, timedelta

from fastapi.testclient import TestClient

from data_autopilot.db.session import SessionLocal
from data_autopilot.main import app
from data_autopilot.models.entities import AlertEvent, AlertStatus


client = TestClient(app)


def test_alert_policy_update_and_read() -> None:
    org = "org_alert_policy"
    admin_headers = {"X-Tenant-Id": org, "X-User-Role": "admin"}
    member_headers = {"X-Tenant-Id": org, "X-User-Role": "member"}

    set_resp = client.post(
        "/api/v1/alerts/policy",
        headers=admin_headers,
        json={"org_id": org, "policy": {"P1": 5, "P2": 20}},
    )
    assert set_resp.status_code == 200
    body = set_resp.json()
    assert body["policy"]["P1"] == 5
    assert body["policy"]["P2"] == 20

    get_resp = client.get("/api/v1/alerts/policy", params={"org_id": org}, headers=member_headers)
    assert get_resp.status_code == 200
    assert get_resp.json()["policy"]["P1"] == 5


def test_snoozed_alert_not_escalated_until_expiry() -> None:
    org = "org_alert_snooze"
    admin_headers = {"X-Tenant-Id": org, "X-User-Role": "admin"}
    member_headers = {"X-Tenant-Id": org, "X-User-Role": "member"}

    # Speed up escalation for deterministic checks.
    client.post(
        "/api/v1/alerts/policy",
        headers=admin_headers,
        json={"org_id": org, "policy": {"P1": 1}},
    )

    created = client.post(
        "/api/v1/alerts",
        headers=member_headers,
        json={
            "org_id": org,
            "dedupe_key": "dq.snooze.test",
            "title": "Snooze Test",
            "message": "Testing snooze behavior",
            "severity": "P1",
        },
    )
    assert created.status_code == 200
    alert_id = created.json()["alert_id"]

    snooze = client.post(
        f"/api/v1/alerts/{alert_id}/snooze",
        params={"org_id": org},
        headers=member_headers,
        json={"user_id": "u_snooze", "duration_minutes": 120, "reason": "maintenance window"},
    )
    assert snooze.status_code == 200
    assert snooze.json()["snoozed_until"] is not None

    # Make alert otherwise due for escalation, but keep snooze in the future.
    db = SessionLocal()
    try:
        row = db.query(AlertEvent).filter(AlertEvent.id == alert_id, AlertEvent.tenant_id == org).one()
        row.status = AlertStatus.OPEN
        row.next_escalation_at = datetime.utcnow() - timedelta(minutes=2)
        row.snoozed_until = datetime.utcnow() + timedelta(minutes=60)
        db.add(row)
        db.commit()
    finally:
        db.close()

    escalated = client.post("/api/v1/alerts/escalate", params={"org_id": org}, headers=member_headers)
    assert escalated.status_code == 200
    assert alert_id not in escalated.json()["alert_ids"]

    # Expire snooze and ensure escalation now includes it.
    db = SessionLocal()
    try:
        row = db.query(AlertEvent).filter(AlertEvent.id == alert_id, AlertEvent.tenant_id == org).one()
        row.snoozed_until = datetime.utcnow() - timedelta(minutes=1)
        row.next_escalation_at = datetime.utcnow() - timedelta(minutes=1)
        db.add(row)
        db.commit()
    finally:
        db.close()

    escalated2 = client.post("/api/v1/alerts/escalate", params={"org_id": org}, headers=member_headers)
    assert escalated2.status_code == 200
    assert alert_id in escalated2.json()["alert_ids"]
