from sqlalchemy.orm import Session

from data_autopilot.models.entities import AuditLog


class AuditService:
    def log(self, db: Session, tenant_id: str, event_type: str, payload: dict) -> None:
        db.add(AuditLog(tenant_id=tenant_id, event_type=event_type, payload=payload))
        db.commit()

    def list_recent(self, db: Session, tenant_id: str, limit: int = 100) -> list[AuditLog]:
        return (
            db.query(AuditLog)
            .filter(AuditLog.tenant_id == tenant_id)
            .order_by(AuditLog.created_at.desc())
            .limit(limit)
            .all()
        )
