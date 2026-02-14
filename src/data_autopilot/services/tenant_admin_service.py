from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from data_autopilot.models.entities import (
    AlertEvent,
    AlertNotification,
    Artifact,
    ArtifactVersion,
    AuditLog,
    CatalogColumn,
    CatalogTable,
    Connection,
    Feedback,
    QueryApproval,
    Tenant,
    User,
    WorkflowDeadLetter,
    WorkflowQueue,
    WorkflowRun,
    WorkflowStep,
)
from data_autopilot.services.audit import AuditService
from data_autopilot.services.cache_service import CacheService


@dataclass
class TenantPurgePreview:
    tenant_exists: bool
    active_workflows: int
    queued_workflows: int
    counts: dict[str, int]
    estimated_cache_entries: int
    audit_rows_retained: int


class TenantAdminService:
    def __init__(self) -> None:
        self.cache = CacheService()
        self.audit = AuditService()

    def preview(self, db: Session, tenant_id: str) -> TenantPurgePreview:
        tenant_exists = db.execute(select(Tenant).where(Tenant.id == tenant_id)).scalar_one_or_none() is not None
        active_workflows = self._count(db, select(func.count(WorkflowRun.id)).where(WorkflowRun.tenant_id == tenant_id, WorkflowRun.status == "running"))
        queued_workflows = self._count(
            db,
            select(func.count(WorkflowQueue.id)).where(WorkflowQueue.tenant_id == tenant_id, WorkflowQueue.status == "queued"),
        )
        counts = {
            "connections": self._count(db, select(func.count(Connection.id)).where(Connection.tenant_id == tenant_id)),
            "catalog_tables": self._count(db, select(func.count(CatalogTable.id)).where(CatalogTable.tenant_id == tenant_id)),
            "catalog_columns": self._count(db, select(func.count(CatalogColumn.id)).where(CatalogColumn.tenant_id == tenant_id)),
            "artifacts": self._count(db, select(func.count(Artifact.id)).where(Artifact.tenant_id == tenant_id)),
            "artifact_versions": self._count(db, select(func.count(ArtifactVersion.id)).where(ArtifactVersion.tenant_id == tenant_id)),
            "feedback": self._count(db, select(func.count(Feedback.id)).where(Feedback.tenant_id == tenant_id)),
            "workflow_runs": self._count(db, select(func.count(WorkflowRun.id)).where(WorkflowRun.tenant_id == tenant_id)),
            "workflow_steps": self._count(
                db,
                select(func.count(WorkflowStep.id)).where(
                    WorkflowStep.workflow_id.in_(select(WorkflowRun.id).where(WorkflowRun.tenant_id == tenant_id))
                ),
            ),
            "workflow_queue": self._count(db, select(func.count(WorkflowQueue.id)).where(WorkflowQueue.tenant_id == tenant_id)),
            "workflow_dead_letters": self._count(
                db, select(func.count(WorkflowDeadLetter.id)).where(WorkflowDeadLetter.tenant_id == tenant_id)
            ),
            "query_approvals": self._count(db, select(func.count(QueryApproval.id)).where(QueryApproval.tenant_id == tenant_id)),
            "alerts": self._count(db, select(func.count(AlertEvent.id)).where(AlertEvent.tenant_id == tenant_id)),
            "alert_notifications": self._count(
                db, select(func.count(AlertNotification.id)).where(AlertNotification.tenant_id == tenant_id)
            ),
            "users": self._count(db, select(func.count(User.id)).where(User.tenant_id == tenant_id)),
            "tenant_rows": self._count(db, select(func.count(Tenant.id)).where(Tenant.id == tenant_id)),
        }
        audit_rows_retained = self._count(db, select(func.count(AuditLog.id)).where(AuditLog.tenant_id == tenant_id))
        estimated_cache_entries = self._estimate_cache_entries(tenant_id)
        return TenantPurgePreview(
            tenant_exists=tenant_exists,
            active_workflows=active_workflows,
            queued_workflows=queued_workflows,
            counts=counts,
            estimated_cache_entries=estimated_cache_entries,
            audit_rows_retained=audit_rows_retained,
        )

    def purge(self, db: Session, tenant_id: str, force: bool = False) -> dict:
        preview = self.preview(db, tenant_id)
        if not preview.tenant_exists:
            return {"status": "not_found", "tenant_id": tenant_id}
        if not force and preview.active_workflows > 0:
            return {
                "status": "blocked_active_workflows",
                "tenant_id": tenant_id,
                "active_workflows": preview.active_workflows,
            }

        # Write a final lifecycle marker before deleting tenant-scoped rows.
        self.audit.log(
            db,
            tenant_id=tenant_id,
            event_type="tenant_purge_executed",
            payload={
                "tenant_id": tenant_id,
                "force": force,
                "started_at": datetime.utcnow().isoformat(),
                "preview_counts": preview.counts,
            },
        )
        db.commit()

        run_ids = [row[0] for row in db.execute(select(WorkflowRun.id).where(WorkflowRun.tenant_id == tenant_id)).all()]
        deleted = {}
        if run_ids:
            deleted["workflow_steps"] = db.execute(
                delete(WorkflowStep).where(WorkflowStep.workflow_id.in_(run_ids))
            ).rowcount or 0
        else:
            deleted["workflow_steps"] = 0
        deleted["alert_notifications"] = db.execute(
            delete(AlertNotification).where(AlertNotification.tenant_id == tenant_id)
        ).rowcount or 0
        deleted["alerts"] = db.execute(delete(AlertEvent).where(AlertEvent.tenant_id == tenant_id)).rowcount or 0
        deleted["query_approvals"] = db.execute(
            delete(QueryApproval).where(QueryApproval.tenant_id == tenant_id)
        ).rowcount or 0
        deleted["workflow_dead_letters"] = db.execute(
            delete(WorkflowDeadLetter).where(WorkflowDeadLetter.tenant_id == tenant_id)
        ).rowcount or 0
        deleted["workflow_queue"] = db.execute(
            delete(WorkflowQueue).where(WorkflowQueue.tenant_id == tenant_id)
        ).rowcount or 0
        deleted["workflow_runs"] = db.execute(delete(WorkflowRun).where(WorkflowRun.tenant_id == tenant_id)).rowcount or 0
        deleted["feedback"] = db.execute(delete(Feedback).where(Feedback.tenant_id == tenant_id)).rowcount or 0
        deleted["artifact_versions"] = db.execute(
            delete(ArtifactVersion).where(ArtifactVersion.tenant_id == tenant_id)
        ).rowcount or 0
        deleted["artifacts"] = db.execute(delete(Artifact).where(Artifact.tenant_id == tenant_id)).rowcount or 0
        deleted["catalog_columns"] = db.execute(
            delete(CatalogColumn).where(CatalogColumn.tenant_id == tenant_id)
        ).rowcount or 0
        deleted["catalog_tables"] = db.execute(
            delete(CatalogTable).where(CatalogTable.tenant_id == tenant_id)
        ).rowcount or 0
        deleted["connections"] = db.execute(delete(Connection).where(Connection.tenant_id == tenant_id)).rowcount or 0
        deleted["users"] = db.execute(delete(User).where(User.tenant_id == tenant_id)).rowcount or 0
        deleted["tenant_rows"] = db.execute(delete(Tenant).where(Tenant.id == tenant_id)).rowcount or 0
        db.commit()

        cache_deleted = self._purge_tenant_cache(tenant_id)
        return {
            "status": "purged",
            "tenant_id": tenant_id,
            "deleted": deleted,
            "cache_entries_purged": cache_deleted,
            "audit_rows_retained": preview.audit_rows_retained + 1,
        }

    @staticmethod
    def _count(db: Session, stmt) -> int:
        return int(db.execute(stmt).scalar_one() or 0)

    def _estimate_cache_entries(self, tenant_id: str) -> int:
        prefixes = [
            f"cost_budget:{tenant_id}",
            f"query:{tenant_id}:",
            f"session:{tenant_id}:",
            f"tenant:{tenant_id}:",
        ]
        return sum(self.cache.store.count_prefix(prefix) for prefix in prefixes)

    def _purge_tenant_cache(self, tenant_id: str) -> int:
        prefixes = [
            f"cost_budget:{tenant_id}",
            f"query:{tenant_id}:",
            f"session:{tenant_id}:",
            f"tenant:{tenant_id}:",
        ]
        return sum(self.cache.store.delete_prefix(prefix) for prefix in prefixes)
