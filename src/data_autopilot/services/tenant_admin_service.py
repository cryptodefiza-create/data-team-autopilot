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
    CACHE_PREFIX_TEMPLATES = (
        "cost_budget:{tenant_id}",
        "query:{tenant_id}:",
        "session:{tenant_id}:",
        "tenant:{tenant_id}:",
    )

    PREVIEW_COUNT_QUERIES = {
        "connections": lambda tenant_id: select(func.count(Connection.id)).where(Connection.tenant_id == tenant_id),
        "catalog_tables": lambda tenant_id: select(func.count(CatalogTable.id)).where(CatalogTable.tenant_id == tenant_id),
        "catalog_columns": lambda tenant_id: select(func.count(CatalogColumn.id)).where(CatalogColumn.tenant_id == tenant_id),
        "artifacts": lambda tenant_id: select(func.count(Artifact.id)).where(Artifact.tenant_id == tenant_id),
        "artifact_versions": lambda tenant_id: select(func.count(ArtifactVersion.id)).where(ArtifactVersion.tenant_id == tenant_id),
        "feedback": lambda tenant_id: select(func.count(Feedback.id)).where(Feedback.tenant_id == tenant_id),
        "workflow_runs": lambda tenant_id: select(func.count(WorkflowRun.id)).where(WorkflowRun.tenant_id == tenant_id),
        "workflow_queue": lambda tenant_id: select(func.count(WorkflowQueue.id)).where(WorkflowQueue.tenant_id == tenant_id),
        "workflow_dead_letters": lambda tenant_id: select(func.count(WorkflowDeadLetter.id)).where(WorkflowDeadLetter.tenant_id == tenant_id),
        "query_approvals": lambda tenant_id: select(func.count(QueryApproval.id)).where(QueryApproval.tenant_id == tenant_id),
        "alerts": lambda tenant_id: select(func.count(AlertEvent.id)).where(AlertEvent.tenant_id == tenant_id),
        "alert_notifications": lambda tenant_id: select(func.count(AlertNotification.id)).where(AlertNotification.tenant_id == tenant_id),
        "users": lambda tenant_id: select(func.count(User.id)).where(User.tenant_id == tenant_id),
        "tenant_rows": lambda tenant_id: select(func.count(Tenant.id)).where(Tenant.id == tenant_id),
    }

    DELETE_MODELS = (
        ("alert_notifications", AlertNotification, "tenant_id"),
        ("alerts", AlertEvent, "tenant_id"),
        ("query_approvals", QueryApproval, "tenant_id"),
        ("workflow_dead_letters", WorkflowDeadLetter, "tenant_id"),
        ("workflow_queue", WorkflowQueue, "tenant_id"),
        ("workflow_runs", WorkflowRun, "tenant_id"),
        ("feedback", Feedback, "tenant_id"),
        ("artifact_versions", ArtifactVersion, "tenant_id"),
        ("artifacts", Artifact, "tenant_id"),
        ("catalog_columns", CatalogColumn, "tenant_id"),
        ("catalog_tables", CatalogTable, "tenant_id"),
        ("connections", Connection, "tenant_id"),
        ("users", User, "tenant_id"),
        ("tenant_rows", Tenant, "id"),
    )

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
        counts = {name: self._count(db, query(tenant_id)) for name, query in self.PREVIEW_COUNT_QUERIES.items()}
        counts["workflow_steps"] = self._count(
            db,
            select(func.count(WorkflowStep.id)).where(
                WorkflowStep.workflow_id.in_(select(WorkflowRun.id).where(WorkflowRun.tenant_id == tenant_id))
            ),
        )
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
        for key, model, attr in self.DELETE_MODELS:
            deleted[key] = db.execute(delete(model).where(getattr(model, attr) == tenant_id)).rowcount or 0
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
        return sum(self.cache.store.count_prefix(prefix) for prefix in self._cache_prefixes(tenant_id))

    def _purge_tenant_cache(self, tenant_id: str) -> int:
        return sum(self.cache.store.delete_prefix(prefix) for prefix in self._cache_prefixes(tenant_id))

    def _cache_prefixes(self, tenant_id: str) -> list[str]:
        return [template.format(tenant_id=tenant_id) for template in self.CACHE_PREFIX_TEMPLATES]
