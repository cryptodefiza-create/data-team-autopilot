from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from data_autopilot.db.base import Base
from data_autopilot.models.entities import Tenant


@dataclass
class MigrationSummary:
    created_tables: int
    compatibility_changes: list[str]
    tenants_checked: list[str]
    errors: list[str]


class MigrationRunner:
    """Tenant-aware migration runner for local/dev and staged environments."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def run(self, db: Session) -> MigrationSummary:
        created_before = set(inspect(self.engine).get_table_names())
        Base.metadata.create_all(bind=self.engine)
        created_after = set(inspect(self.engine).get_table_names())
        created_tables = len(created_after - created_before)

        compatibility_changes = self._compatibility_migrations()
        tenants_checked, errors = self._tenant_checks(db)

        return MigrationSummary(
            created_tables=created_tables,
            compatibility_changes=compatibility_changes,
            tenants_checked=tenants_checked,
            errors=errors,
        )

    def _compatibility_migrations(self) -> list[str]:
        changes: list[str] = []
        dialect = self.engine.dialect.name

        with self.engine.begin() as conn:
            if "workflow_queue" in inspect(self.engine).get_table_names():
                cols = {c["name"] for c in inspect(self.engine).get_columns("workflow_queue")}
                if "attempts" not in cols:
                    if dialect == "sqlite":
                        conn.execute(text("ALTER TABLE workflow_queue ADD COLUMN attempts INTEGER DEFAULT 0"))
                    else:
                        conn.execute(text("ALTER TABLE workflow_queue ADD COLUMN IF NOT EXISTS attempts INTEGER DEFAULT 0"))
                    changes.append("workflow_queue.attempts")
                if "error_history" not in cols:
                    if dialect == "sqlite":
                        conn.execute(text("ALTER TABLE workflow_queue ADD COLUMN error_history JSON DEFAULT '[]'"))
                    else:
                        conn.execute(text("ALTER TABLE workflow_queue ADD COLUMN IF NOT EXISTS error_history JSON DEFAULT '[]'"))
                    changes.append("workflow_queue.error_history")
            if "alerts" in inspect(self.engine).get_table_names():
                cols = {c["name"] for c in inspect(self.engine).get_columns("alerts")}
                if "snoozed_until" not in cols:
                    if dialect == "sqlite":
                        conn.execute(text("ALTER TABLE alerts ADD COLUMN snoozed_until DATETIME"))
                    else:
                        conn.execute(text("ALTER TABLE alerts ADD COLUMN IF NOT EXISTS snoozed_until TIMESTAMP"))
                    changes.append("alerts.snoozed_until")
                if "snoozed_by" not in cols:
                    if dialect == "sqlite":
                        conn.execute(text("ALTER TABLE alerts ADD COLUMN snoozed_by VARCHAR(64)"))
                    else:
                        conn.execute(text("ALTER TABLE alerts ADD COLUMN IF NOT EXISTS snoozed_by VARCHAR(64)"))
                    changes.append("alerts.snoozed_by")
                if "snoozed_reason" not in cols:
                    if dialect == "sqlite":
                        conn.execute(text("ALTER TABLE alerts ADD COLUMN snoozed_reason VARCHAR(255)"))
                    else:
                        conn.execute(text("ALTER TABLE alerts ADD COLUMN IF NOT EXISTS snoozed_reason VARCHAR(255)"))
                    changes.append("alerts.snoozed_reason")
            if "alert_notifications" in inspect(self.engine).get_table_names():
                cols = {c["name"] for c in inspect(self.engine).get_columns("alert_notifications")}
                if "retry_count" not in cols:
                    if dialect == "sqlite":
                        conn.execute(text("ALTER TABLE alert_notifications ADD COLUMN retry_count INTEGER DEFAULT 0"))
                    else:
                        conn.execute(text("ALTER TABLE alert_notifications ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 0"))
                    changes.append("alert_notifications.retry_count")
                if "next_retry_at" not in cols:
                    if dialect == "sqlite":
                        conn.execute(text("ALTER TABLE alert_notifications ADD COLUMN next_retry_at DATETIME"))
                    else:
                        conn.execute(text("ALTER TABLE alert_notifications ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMP"))
                    changes.append("alert_notifications.next_retry_at")
                if "last_error" not in cols:
                    if dialect == "sqlite":
                        conn.execute(text("ALTER TABLE alert_notifications ADD COLUMN last_error VARCHAR(255)"))
                    else:
                        conn.execute(text("ALTER TABLE alert_notifications ADD COLUMN IF NOT EXISTS last_error VARCHAR(255)"))
                    changes.append("alert_notifications.last_error")

        return changes

    def _tenant_checks(self, db: Session) -> tuple[list[str], list[str]]:
        checked: list[str] = []
        errors: list[str] = []
        tenants = db.query(Tenant).all()
        for tenant in tenants:
            tenant_id = str(tenant.id)
            checked.append(tenant_id)
            try:
                # Current data model uses tenant_id row isolation instead of physical schemas.
                # This check ensures core tenant-scoped tables are queryable for each tenant.
                db.execute(text("SELECT COUNT(*) FROM workflow_runs WHERE tenant_id = :tenant_id"), {"tenant_id": tenant_id})
                db.execute(text("SELECT COUNT(*) FROM artifacts WHERE tenant_id = :tenant_id"), {"tenant_id": tenant_id})
            except Exception as exc:  # pragma: no cover
                errors.append(f"{tenant_id}: {exc}")
        return checked, errors

    @staticmethod
    def as_dict(summary: MigrationSummary) -> dict[str, Any]:
        return {
            "created_tables": summary.created_tables,
            "compatibility_changes": summary.compatibility_changes,
            "tenants_checked": summary.tenants_checked,
            "errors": summary.errors,
            "ok": len(summary.errors) == 0,
        }
