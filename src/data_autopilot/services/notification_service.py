from __future__ import annotations

from datetime import datetime, timedelta
from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from data_autopilot.models.entities import (
    AlertEvent,
    AlertNotification,
    AlertNotificationStatus,
    AlertStatus,
    Tenant,
)


class NotificationService:
    def __init__(self) -> None:
        self.max_retries = 3
        self.retry_backoff_minutes = [1, 4, 16]

    def _tenant(self, db: Session, tenant_id: str) -> Tenant:
        tenant = db.execute(select(Tenant).where(Tenant.id == tenant_id)).scalar_one_or_none()
        if tenant is None:
            tenant = Tenant(id=tenant_id, name=tenant_id, settings={})
            db.add(tenant)
            db.commit()
            db.refresh(tenant)
        return tenant

    def get_routing(self, db: Session, tenant_id: str) -> dict:
        tenant = self._tenant(db, tenant_id)
        settings = dict(tenant.settings or {})
        default = {
            "channels": [],
            "escalation_recipients": {"P0": [], "P1": [], "P2": [], "P3": []},
            "ack_reminder_minutes": 30,
        }
        routing = settings.get("notification_routing", {})
        if not isinstance(routing, dict):
            return default
        return {
            "channels": routing.get("channels", []),
            "escalation_recipients": routing.get("escalation_recipients", default["escalation_recipients"]),
            "ack_reminder_minutes": int(routing.get("ack_reminder_minutes", 30)),
        }

    def set_routing(self, db: Session, tenant_id: str, routing: dict) -> dict:
        tenant = self._tenant(db, tenant_id)
        channels = routing.get("channels", [])
        recipients = routing.get("escalation_recipients", {})
        reminder = int(routing.get("ack_reminder_minutes", 30))
        sanitized = {
            "channels": channels if isinstance(channels, list) else [],
            "escalation_recipients": recipients if isinstance(recipients, dict) else {"P0": [], "P1": [], "P2": [], "P3": []},
            "ack_reminder_minutes": max(1, reminder),
        }
        settings = dict(tenant.settings or {})
        settings["notification_routing"] = sanitized
        tenant.settings = settings
        db.add(tenant)
        db.commit()
        return sanitized

    def _attempt_send(self, channel_target: str, retry_count: int) -> tuple[bool, str | None]:
        target = channel_target.strip().lower()
        if target.startswith("fail://"):
            return False, "simulated delivery failure"
        if target.startswith("flaky://") and retry_count < 1:
            return False, "simulated transient delivery failure"
        return True, None

    def _next_retry(self, retry_count: int) -> datetime:
        idx = min(max(retry_count, 0), len(self.retry_backoff_minutes) - 1)
        return datetime.utcnow() + timedelta(minutes=self.retry_backoff_minutes[idx])

    def _build_notification(
        self,
        alert: AlertEvent,
        event_type: str,
        channel_type: str,
        target: str,
        recipient: str | None,
    ) -> AlertNotification:
        sent, error = self._attempt_send(target, retry_count=0)
        status = AlertNotificationStatus.SENT if sent else AlertNotificationStatus.FAILED
        return AlertNotification(
            id=f"ntf_{uuid4().hex[:12]}",
            tenant_id=alert.tenant_id,
            alert_id=alert.id,
            severity=alert.severity,
            event_type=event_type,
            channel_type=channel_type,
            channel_target=target,
            recipient=recipient,
            payload={"title": alert.title, "message": alert.message},
            status=status,
            retry_count=0,
            next_retry_at=None if sent else self._next_retry(0),
            last_error=error,
            sent_at=datetime.utcnow() if sent else None,
        )

    def queue_for_alert(self, db: Session, alert: AlertEvent, event_type: str) -> list[AlertNotification]:
        routing = self.get_routing(db, alert.tenant_id)
        channels = routing.get("channels", [])
        recips_by_sev = routing.get("escalation_recipients", {})
        recipients = recips_by_sev.get(alert.severity.value, []) if isinstance(recips_by_sev, dict) else []
        if not isinstance(recipients, list):
            recipients = []

        rows: list[AlertNotification] = []
        for ch in channels:
            if not isinstance(ch, dict):
                continue
            if not ch.get("enabled", True):
                continue
            sev = ch.get("severities", ["P0", "P1", "P2", "P3"])
            if isinstance(sev, list) and alert.severity.value not in sev:
                continue
            ch_type = str(ch.get("type", "email"))
            target = str(ch.get("target", "")).strip()
            if not target:
                continue
            if recipients:
                for recipient in recipients:
                    row = self._build_notification(
                        alert=alert,
                        event_type=event_type,
                        channel_type=ch_type,
                        target=target,
                        recipient=str(recipient),
                    )
                    db.add(row)
                    rows.append(row)
            else:
                row = self._build_notification(
                    alert=alert,
                    event_type=event_type,
                    channel_type=ch_type,
                    target=target,
                    recipient=None,
                )
                db.add(row)
                rows.append(row)
        if rows:
            db.commit()
        return rows

    def list_notifications(self, db: Session, tenant_id: str, alert_id: str | None = None) -> list[AlertNotification]:
        stmt = select(AlertNotification).where(AlertNotification.tenant_id == tenant_id).order_by(AlertNotification.created_at.desc())
        if alert_id:
            stmt = stmt.where(AlertNotification.alert_id == alert_id)
        return list(db.execute(stmt).scalars().all())

    def queue_ack_reminders(self, db: Session, tenant_id: str) -> list[AlertNotification]:
        routing = self.get_routing(db, tenant_id)
        reminder_minutes = int(routing.get("ack_reminder_minutes", 30))
        cutoff = datetime.utcnow() - timedelta(minutes=reminder_minutes)
        alerts = db.execute(
            select(AlertEvent).where(
                AlertEvent.tenant_id == tenant_id,
                AlertEvent.status.in_([AlertStatus.OPEN, AlertStatus.ESCALATED]),
                AlertEvent.acknowledged_at.is_(None),
                AlertEvent.last_notified_at <= cutoff,
            )
        ).scalars().all()

        reminders: list[AlertNotification] = []
        for alert in alerts:
            reminders.extend(self.queue_for_alert(db, alert, event_type="reminder"))
            alert.last_notified_at = datetime.utcnow()
            db.add(alert)
        if alerts:
            db.commit()
        return reminders

    def retry_failed_notifications(self, db: Session, tenant_id: str, now: datetime | None = None) -> list[AlertNotification]:
        current = now or datetime.utcnow()
        rows = db.execute(
            select(AlertNotification).where(
                AlertNotification.tenant_id == tenant_id,
                AlertNotification.status == AlertNotificationStatus.FAILED,
                AlertNotification.retry_count < self.max_retries,
                AlertNotification.next_retry_at.is_not(None),
                AlertNotification.next_retry_at <= current,
            )
        ).scalars().all()
        retried: list[AlertNotification] = []
        for row in rows:
            attempt = int(row.retry_count or 0) + 1
            sent, error = self._attempt_send(row.channel_target, retry_count=attempt)
            if sent:
                row.status = AlertNotificationStatus.SENT
                row.sent_at = datetime.utcnow()
                row.retry_count = attempt
                row.next_retry_at = None
                row.last_error = None
            else:
                row.status = AlertNotificationStatus.FAILED
                row.retry_count = attempt
                if attempt >= self.max_retries:
                    row.next_retry_at = None
                else:
                    row.next_retry_at = self._next_retry(attempt)
                row.last_error = error
            db.add(row)
            retried.append(row)
        if retried:
            db.commit()
        return retried

    def metrics(self, db: Session, tenant_id: str) -> dict:
        total = int(
            db.execute(
                select(func.count(AlertNotification.id)).where(AlertNotification.tenant_id == tenant_id)
            ).scalar_one()
            or 0
        )
        by_status = db.execute(
            select(AlertNotification.status, func.count(AlertNotification.id))
            .where(AlertNotification.tenant_id == tenant_id)
            .group_by(AlertNotification.status)
        ).all()
        counters = {"queued": 0, "sent": 0, "failed": 0}
        for status, count in by_status:
            key = status.value if hasattr(status, "value") else str(status)
            counters[key] = int(count)

        retry_backlog = int(
            db.execute(
                select(func.count(AlertNotification.id)).where(
                    AlertNotification.tenant_id == tenant_id,
                    AlertNotification.status == AlertNotificationStatus.FAILED,
                    AlertNotification.next_retry_at.is_not(None),
                )
            ).scalar_one()
            or 0
        )
        success_rate = round((counters.get("sent", 0) / total) * 100, 2) if total else 0.0
        return {
            "tenant_id": tenant_id,
            "total": total,
            "sent": counters.get("sent", 0),
            "failed": counters.get("failed", 0),
            "queued": counters.get("queued", 0),
            "retry_backlog": retry_backlog,
            "success_rate_pct": success_rate,
        }
