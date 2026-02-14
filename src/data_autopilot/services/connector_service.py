from __future__ import annotations

from datetime import datetime

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from data_autopilot.models.entities import Artifact, CatalogColumn, CatalogTable, Connection, WorkflowQueue, WorkflowRun
from data_autopilot.services.audit import AuditService
from data_autopilot.services.bigquery_connector import BigQueryConnector
from data_autopilot.services.cache_service import CacheService
from data_autopilot.services.secrets_manager import SecretsManager


class ConnectorService:
    def __init__(self) -> None:
        self.secrets = SecretsManager()
        self.cache = CacheService()
        self.connector = BigQueryConnector()
        self.audit = AuditService()

    def connect(self, db: Session, org_id: str, service_account_json: dict) -> Connection:
        status = self.connector.test_connection(service_account_json=service_account_json)
        if not status.get("ok"):
            raise RuntimeError("BigQuery connection test failed")

        connection_id = f"conn_{org_id}"
        existing = db.execute(select(Connection).where(Connection.id == connection_id)).scalar_one_or_none()
        encrypted = self.secrets.encrypt(service_account_json)

        if existing is None:
            row = Connection(
                id=connection_id,
                tenant_id=org_id,
                status="active",
                config_encrypted=encrypted,
            )
            db.add(row)
            db.commit()
            db.refresh(row)
            self.audit.log(
                db,
                tenant_id=org_id,
                event_type="connector_connected",
                payload={"connection_id": row.id, "status": row.status, "created": True},
            )
            return row

        existing.config_encrypted = encrypted
        existing.status = "active"
        db.add(existing)
        db.commit()
        db.refresh(existing)
        self.audit.log(
            db,
            tenant_id=org_id,
            event_type="connector_connected",
            payload={"connection_id": existing.id, "status": existing.status, "created": False},
        )
        return existing

    def disconnect(self, db: Session, org_id: str, connection_id: str) -> dict:
        row = db.execute(
            select(Connection).where(Connection.id == connection_id, Connection.tenant_id == org_id)
        ).scalar_one_or_none()
        if row is None:
            return {"status": "not_found", "connection_id": connection_id}

        active_runs = db.execute(
            select(WorkflowRun).where(WorkflowRun.tenant_id == org_id, WorkflowRun.status == "running")
        ).scalars().all()
        cancelled_run_ids: list[str] = []
        for run in active_runs:
            run.status = "cancelled"
            run.finished_at = datetime.utcnow()
            db.add(run)
            cancelled_run_ids.append(run.id)

        queued = db.execute(
            select(WorkflowQueue).where(WorkflowQueue.tenant_id == org_id, WorkflowQueue.status == "queued")
        ).scalars().all()
        cancelled_queue_ids: list[str] = []
        for q in queued:
            q.status = "cancelled"
            q.processed_at = datetime.utcnow()
            db.add(q)
            cancelled_queue_ids.append(q.id)

        row.status = "disconnected"
        row.config_encrypted = {}
        db.add(row)

        db.execute(delete(CatalogTable).where(CatalogTable.connection_id == connection_id, CatalogTable.tenant_id == org_id))
        db.execute(delete(CatalogColumn).where(CatalogColumn.connection_id == connection_id, CatalogColumn.tenant_id == org_id))

        arts = db.execute(select(Artifact).where(Artifact.tenant_id == org_id)).scalars().all()
        for artifact in arts:
            artifact.stale = True
            db.add(artifact)

        db.commit()

        purged = self.cache.invalidate_connection(connection_id)
        payload = {
            "status": "disconnected",
            "connection_id": connection_id,
            "cache_entries_purged": purged,
            "cancelled_active_workflows": len(cancelled_run_ids),
            "cancelled_queued_workflows": len(cancelled_queue_ids),
        }
        self.audit.log(db, tenant_id=org_id, event_type="connector_disconnected", payload=payload)
        return payload
