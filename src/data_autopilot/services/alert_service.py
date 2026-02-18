from __future__ import annotations

from datetime import datetime, timedelta
from uuid import uuid4

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session

import logging

from data_autopilot.models.entities import AlertEvent, AlertSeverity, AlertStatus, Tenant

logger = logging.getLogger(__name__)


class AlertService:
    def __init__(self) -> None:
        self.default_escalation_minutes = {
            AlertSeverity.P0: 15,
            AlertSeverity.P1: 30,
            AlertSeverity.P2: 60,
            AlertSeverity.P3: 120,
        }

    def _policy(self, db: Session, tenant_id: str) -> dict[AlertSeverity, int]:
        policy = dict(self.default_escalation_minutes)
        tenant = db.execute(select(Tenant).where(Tenant.id == tenant_id)).scalar_one_or_none()
        if tenant is None:
            return policy
        settings = dict(tenant.settings or {})
        configured = settings.get("alert_policy", {})
        if not isinstance(configured, dict):
            return policy
        for sev in [AlertSeverity.P0, AlertSeverity.P1, AlertSeverity.P2, AlertSeverity.P3]:
            key = sev.value
            if key in configured:
                try:
                    minutes = int(configured[key])
                    if minutes > 0:
                        policy[sev] = minutes
                except Exception:
                    logger.debug("Invalid alert policy value for %s: %r", key, configured[key])
                    continue
        return policy

    def _next_escalation(self, db: Session, tenant_id: str, severity: AlertSeverity, now: datetime) -> datetime:
        policy = self._policy(db, tenant_id)
        return now + timedelta(minutes=policy[severity])

    def create_or_update(
        self,
        db: Session,
        tenant_id: str,
        dedupe_key: str,
        title: str,
        message: str,
        severity: AlertSeverity,
        source_type: str = "system",
        source_id: str | None = None,
    ) -> AlertEvent:
        now = datetime.utcnow()
        existing = db.execute(
            select(AlertEvent).where(
                AlertEvent.tenant_id == tenant_id,
                AlertEvent.dedupe_key == dedupe_key,
                AlertEvent.status.in_([AlertStatus.OPEN, AlertStatus.ESCALATED, AlertStatus.ACKNOWLEDGED]),
            )
        ).scalar_one_or_none()
        if existing is not None:
            existing.title = title
            existing.message = message
            existing.severity = severity
            existing.last_notified_at = now
            if existing.status != AlertStatus.ACKNOWLEDGED:
                existing.next_escalation_at = self._next_escalation(db, tenant_id, severity, now)
                existing.status = AlertStatus.OPEN
            existing.snoozed_until = None
            existing.snoozed_by = None
            existing.snoozed_reason = None
            db.add(existing)
            db.commit()
            db.refresh(existing)
            return existing

        row = AlertEvent(
            id=f"alt_{uuid4().hex[:12]}",
            tenant_id=tenant_id,
            source_type=source_type,
            source_id=source_id,
            dedupe_key=dedupe_key,
            title=title,
            message=message,
            severity=severity,
            status=AlertStatus.OPEN,
            escalated_count=0,
            last_notified_at=now,
            next_escalation_at=self._next_escalation(db, tenant_id, severity, now),
            created_at=now,
        )
        db.add(row)
        db.commit()
        db.refresh(row)
        return row

    def list_for_tenant(self, db: Session, tenant_id: str, status: AlertStatus | None = None) -> list[AlertEvent]:
        stmt = select(AlertEvent).where(AlertEvent.tenant_id == tenant_id).order_by(AlertEvent.created_at.desc())
        if status is not None:
            stmt = stmt.where(AlertEvent.status == status)
        return list(db.execute(stmt).scalars().all())

    def acknowledge(self, db: Session, tenant_id: str, alert_id: str, user_id: str) -> AlertEvent | None:
        row = db.execute(
            select(AlertEvent).where(AlertEvent.tenant_id == tenant_id, AlertEvent.id == alert_id)
        ).scalar_one_or_none()
        if row is None:
            return None
        row.status = AlertStatus.ACKNOWLEDGED
        row.acknowledged_at = datetime.utcnow()
        row.acknowledged_by = user_id
        db.add(row)
        db.commit()
        db.refresh(row)
        return row

    def snooze(
        self,
        db: Session,
        tenant_id: str,
        alert_id: str,
        user_id: str,
        duration_minutes: int,
        reason: str | None = None,
    ) -> AlertEvent | None:
        row = db.execute(
            select(AlertEvent).where(AlertEvent.tenant_id == tenant_id, AlertEvent.id == alert_id)
        ).scalar_one_or_none()
        if row is None:
            return None
        until = datetime.utcnow() + timedelta(minutes=max(1, duration_minutes))
        row.snoozed_until = until
        row.snoozed_by = user_id
        row.snoozed_reason = reason
        row.status = AlertStatus.ACKNOWLEDGED
        row.acknowledged_at = datetime.utcnow()
        row.acknowledged_by = user_id
        db.add(row)
        db.commit()
        db.refresh(row)
        return row

    def resolve(self, db: Session, tenant_id: str, alert_id: str) -> AlertEvent | None:
        row = db.execute(
            select(AlertEvent).where(AlertEvent.tenant_id == tenant_id, AlertEvent.id == alert_id)
        ).scalar_one_or_none()
        if row is None:
            return None
        row.status = AlertStatus.RESOLVED
        row.resolved_at = datetime.utcnow()
        db.add(row)
        db.commit()
        db.refresh(row)
        return row

    def escalate_due(self, db: Session, tenant_id: str, now: datetime | None = None) -> list[AlertEvent]:
        current = now or datetime.utcnow()
        due = db.execute(
            select(AlertEvent).where(
                and_(
                    AlertEvent.tenant_id == tenant_id,
                    AlertEvent.status.in_([AlertStatus.OPEN, AlertStatus.ESCALATED]),
                    AlertEvent.next_escalation_at <= current,
                    or_(AlertEvent.snoozed_until.is_(None), AlertEvent.snoozed_until <= current),
                )
            )
        ).scalars().all()
        rows: list[AlertEvent] = []
        for row in due:
            row.status = AlertStatus.ESCALATED
            row.escalated_count += 1
            row.last_notified_at = current
            row.next_escalation_at = self._next_escalation(db, tenant_id, row.severity, current)
            db.add(row)
            rows.append(row)
        db.commit()
        return rows

    def get_policy(self, db: Session, tenant_id: str) -> dict[str, int]:
        policy = self._policy(db, tenant_id)
        return {k.value: int(v) for k, v in policy.items()}

    def set_policy(self, db: Session, tenant_id: str, policy: dict[str, int]) -> dict[str, int]:
        tenant = db.execute(select(Tenant).where(Tenant.id == tenant_id)).scalar_one_or_none()
        if tenant is None:
            tenant = Tenant(id=tenant_id, name=tenant_id, settings={})
            db.add(tenant)
            db.commit()
            db.refresh(tenant)

        sanitized: dict[str, int] = {}
        for sev in ["P0", "P1", "P2", "P3"]:
            if sev not in policy:
                continue
            try:
                val = int(policy[sev])
            except Exception:
                logger.debug("Invalid policy value for %s: %r", sev, policy[sev])
                continue
            if val > 0:
                sanitized[sev] = val

        settings = dict(tenant.settings or {})
        settings["alert_policy"] = {**settings.get("alert_policy", {}), **sanitized}
        tenant.settings = settings
        db.add(tenant)
        db.commit()
        return self.get_policy(db, tenant_id)
