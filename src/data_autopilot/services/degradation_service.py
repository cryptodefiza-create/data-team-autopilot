from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from data_autopilot.config.settings import get_settings
from data_autopilot.models.entities import WorkflowDeadLetter, WorkflowQueue


class DegradationService:
    def __init__(self) -> None:
        self.settings = get_settings()

    def warehouse_available(self) -> bool:
        return not self.settings.simulate_warehouse_unavailable

    def llm_available(self) -> bool:
        return not self.settings.simulate_llm_unavailable

    def enqueue(self, db: Session, tenant_id: str, workflow_type: str, payload: dict, reason: str) -> dict:
        existing_count = db.execute(
            select(WorkflowQueue).where(
                WorkflowQueue.tenant_id == tenant_id,
                WorkflowQueue.status == "queued",
            )
        ).scalars().all()
        position = len(existing_count) + 1
        row = WorkflowQueue(
            id=f"q_{uuid4().hex[:12]}",
            tenant_id=tenant_id,
            workflow_type=workflow_type,
            payload=payload,
            status="queued",
            reason=reason,
            attempts=0,
            error_history=[],
            created_at=datetime.utcnow(),
        )
        db.add(row)
        db.commit()
        db.refresh(row)
        return {
            "workflow_status": "queued",
            "queue_id": row.id,
            "queue_position": position,
            "reason": reason,
            "message": "Service temporarily unavailable. Request queued and will be processed later.",
        }

    def fetch_queued(self, db: Session, workflow_type: str | None = None, tenant_id: str | None = None) -> list[WorkflowQueue]:
        stmt = select(WorkflowQueue).where(WorkflowQueue.status == "queued")
        if workflow_type:
            stmt = stmt.where(WorkflowQueue.workflow_type == workflow_type)
        if tenant_id:
            stmt = stmt.where(WorkflowQueue.tenant_id == tenant_id)
        return list(db.execute(stmt.order_by(WorkflowQueue.created_at.asc())).scalars().all())

    def fetch_dead_letters(self, db: Session, tenant_id: str) -> list[WorkflowDeadLetter]:
        stmt = select(WorkflowDeadLetter).where(WorkflowDeadLetter.tenant_id == tenant_id).order_by(WorkflowDeadLetter.created_at.desc())
        return list(db.execute(stmt).scalars().all())

    def mark_processed(self, db: Session, queue_row: WorkflowQueue) -> None:
        queue_row.status = "processed"
        queue_row.processed_at = datetime.utcnow()
        db.add(queue_row)
        db.commit()

    def mark_failed_attempt(self, db: Session, queue_row: WorkflowQueue, error: str) -> None:
        history = list(queue_row.error_history or [])
        history.append({"at": datetime.utcnow().isoformat() + "Z", "error": error})
        queue_row.error_history = history
        queue_row.attempts = int(queue_row.attempts or 0) + 1
        db.add(queue_row)
        db.commit()

    def move_to_dead_letter(self, db: Session, queue_row: WorkflowQueue, step_states: list[dict]) -> WorkflowDeadLetter:
        dlq = WorkflowDeadLetter(
            id=f"dlq_{uuid4().hex[:12]}",
            queue_id=queue_row.id,
            tenant_id=queue_row.tenant_id,
            workflow_type=queue_row.workflow_type,
            payload=queue_row.payload,
            step_states=step_states,
            error_history=list(queue_row.error_history or []),
            created_at=datetime.utcnow(),
        )
        queue_row.status = "dead_letter"
        queue_row.processed_at = datetime.utcnow()
        db.add(queue_row)
        db.add(dlq)
        db.commit()
        db.refresh(dlq)
        return dlq
