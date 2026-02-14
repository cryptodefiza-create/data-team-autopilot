from datetime import datetime, timedelta

from fastapi.testclient import TestClient

from data_autopilot.db.session import SessionLocal
from data_autopilot.main import app
from data_autopilot.models.entities import AlertEvent, AlertStatus


client = TestClient(app)


def test_routing_drives_notification_creation_on_alert_create() -> None:
    org = "org_alert_notify_create"
    admin = {"X-Tenant-Id": org, "X-User-Role": "admin"}
    member = {"X-Tenant-Id": org, "X-User-Role": "member"}

    routing = {
        "channels": [
            {"type": "email", "target": "alerts@example.com", "enabled": True, "severities": ["P1", "P0"]},
            {"type": "slack", "target": "#data-alerts", "enabled": True, "severities": ["P1", "P2", "P3", "P0"]},
        ],
        "escalation_recipients": {"P1": ["oncall@dataco.io"]},
        "ack_reminder_minutes": 30,
    }
    set_resp = client.post("/api/v1/alerts/routing", headers=admin, json={"org_id": org, "routing": routing})
    assert set_resp.status_code == 200

    created = client.post(
        "/api/v1/alerts",
        headers=member,
        json={
            "org_id": org,
            "dedupe_key": "dq.notify.create",
            "title": "Create Notification Test",
            "message": "create routing test",
            "severity": "P1",
        },
    )
    assert created.status_code == 200
    alert_id = created.json()["alert_id"]

    notifications = client.get("/api/v1/alerts/notifications", params={"org_id": org, "alert_id": alert_id}, headers=member)
    assert notifications.status_code == 200
    items = notifications.json()["items"]
    assert len(items) >= 2
    assert all(i["event_type"] == "created" for i in items)


def test_reminder_processing_emits_notifications_for_unacked_alerts() -> None:
    org = "org_alert_notify_reminder"
    admin = {"X-Tenant-Id": org, "X-User-Role": "admin"}
    member = {"X-Tenant-Id": org, "X-User-Role": "member"}

    client.post(
        "/api/v1/alerts/routing",
        headers=admin,
        json={
            "org_id": org,
            "routing": {
                "channels": [{"type": "email", "target": "alerts@example.com", "enabled": True, "severities": ["P1"]}],
                "escalation_recipients": {"P1": ["owner@dataco.io"]},
                "ack_reminder_minutes": 1,
            },
        },
    )

    created = client.post(
        "/api/v1/alerts",
        headers=member,
        json={"org_id": org, "dedupe_key": "dq.notify.reminder", "title": "Reminder Test", "message": "ack me", "severity": "P1"},
    )
    assert created.status_code == 200
    alert_id = created.json()["alert_id"]

    db = SessionLocal()
    try:
        row = db.query(AlertEvent).filter(AlertEvent.id == alert_id, AlertEvent.tenant_id == org).one()
        row.status = AlertStatus.OPEN
        row.acknowledged_at = None
        row.last_notified_at = datetime.utcnow() - timedelta(minutes=5)
        db.add(row)
        db.commit()
    finally:
        db.close()

    reminders = client.post("/api/v1/alerts/reminders/process", params={"org_id": org}, headers=member)
    assert reminders.status_code == 200
    assert reminders.json()["reminders"] >= 1

    notifications = client.get("/api/v1/alerts/notifications", params={"org_id": org, "alert_id": alert_id}, headers=member)
    assert notifications.status_code == 200
    assert any(i["event_type"] == "reminder" for i in notifications.json()["items"])


def test_failed_delivery_retry_and_metrics() -> None:
    org = "org_alert_notify_retry"
    admin = {"X-Tenant-Id": org, "X-User-Role": "admin"}
    member = {"X-Tenant-Id": org, "X-User-Role": "member"}

    # flaky target fails once then succeeds on retry
    client.post(
        "/api/v1/alerts/routing",
        headers=admin,
        json={
            "org_id": org,
            "routing": {
                "channels": [{"type": "webhook", "target": "flaky://alerts-webhook", "enabled": True, "severities": ["P2"]}],
                "escalation_recipients": {"P2": ["qa@dataco.io"]},
                "ack_reminder_minutes": 30,
            },
        },
    )

    created = client.post(
        "/api/v1/alerts",
        headers=member,
        json={"org_id": org, "dedupe_key": "dq.notify.retry", "title": "Retry Test", "message": "delivery retry", "severity": "P2"},
    )
    assert created.status_code == 200
    alert_id = created.json()["alert_id"]

    notifs = client.get("/api/v1/alerts/notifications", params={"org_id": org, "alert_id": alert_id}, headers=member)
    assert notifs.status_code == 200
    items = notifs.json()["items"]
    assert len(items) >= 1
    assert items[0]["status"] == "failed"

    # Make it due now for retry
    db = SessionLocal()
    try:
        from data_autopilot.models.entities import AlertNotification, AlertNotificationStatus

        row = (
            db.query(AlertNotification)
            .filter(
                AlertNotification.alert_id == alert_id,
                AlertNotification.status == AlertNotificationStatus.FAILED,
            )
            .order_by(AlertNotification.created_at.desc())
            .first()
        )
        assert row is not None
        row.next_retry_at = datetime.utcnow() - timedelta(minutes=1)
        db.add(row)
        db.commit()
    finally:
        db.close()

    retried = client.post("/api/v1/alerts/notifications/retry", params={"org_id": org}, headers=member)
    assert retried.status_code == 200
    assert retried.json()["retried"] >= 1

    metrics = client.get("/api/v1/alerts/notifications/metrics", params={"org_id": org}, headers=member)
    assert metrics.status_code == 200
    m = metrics.json()
    assert m["total"] >= 1
    assert m["sent"] >= 1
    assert "success_rate_pct" in m
