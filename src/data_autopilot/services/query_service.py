from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from data_autopilot.config.settings import get_settings
from data_autopilot.models.entities import QueryApproval
from data_autopilot.services.bigquery_connector import BigQueryConnector
from data_autopilot.services.connection_context import load_active_connection_credentials
from data_autopilot.services.cost_limiter import SlidingWindowCostLimiter
from data_autopilot.services.sql_safety import SqlSafetyEngine


class QueryService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.safety = SqlSafetyEngine(default_limit=self.settings.default_query_limit)
        self.connector = BigQueryConnector()
        self.cost = SlidingWindowCostLimiter()

    def preview(self, db: Session, tenant_id: str, sql: str) -> dict:
        decision = self.safety.evaluate(sql)
        if not decision.allowed:
            return {"status": "blocked", "reasons": decision.reasons}

        rewritten = decision.rewritten_sql or sql
        _connection_id, creds = load_active_connection_credentials(db, tenant_id=tenant_id)
        if not self.settings.bigquery_mock_mode and creds is None:
            return {"status": "blocked", "reasons": ["No active BigQuery connection for tenant"]}
        dry = self.connector.dry_run(rewritten, service_account_json=creds)
        estimated = int(dry.total_bytes_processed)
        est_cost_cents = int(round(dry.estimated_cost_usd * 100))

        if estimated > self.settings.per_query_max_bytes_with_approval:
            return {"status": "blocked", "reasons": ["Query exceeds hard max bytes with approval"]}

        budget = self.cost.check(tenant_id, estimated)
        if not budget.allowed:
            return {
                "status": "blocked",
                "reasons": ["Hourly budget exceeded"],
                "bytes_remaining": budget.bytes_remaining,
                "budget": budget.budget,
            }

        requires_approval = estimated > self.settings.per_query_max_bytes
        row = QueryApproval(
            id=f"qry_{uuid4().hex[:12]}",
            tenant_id=tenant_id,
            sql=rewritten,
            status="pending" if requires_approval else "approved",
            estimated_bytes=estimated,
            estimated_cost_usd=est_cost_cents,
            requires_approval=requires_approval,
            created_at=datetime.utcnow(),
            approved_at=None if requires_approval else datetime.utcnow(),
        )
        db.add(row)
        db.commit()
        db.refresh(row)
        return {
            "status": "approval_required" if requires_approval else "ready",
            "preview_id": row.id,
            "sql": rewritten,
            "estimated_bytes": estimated,
            "estimated_cost_usd": round(est_cost_cents / 100.0, 4),
            "requires_approval": requires_approval,
        }

    def approve_and_run(self, db: Session, tenant_id: str, preview_id: str) -> dict:
        row = db.execute(
            select(QueryApproval).where(QueryApproval.id == preview_id, QueryApproval.tenant_id == tenant_id)
        ).scalar_one_or_none()
        if row is None:
            return {"status": "not_found"}

        if row.status == "executed":
            return {
                "status": "executed",
                "preview_id": row.id,
                "estimated_bytes": row.estimated_bytes,
                "actual_bytes": row.actual_bytes,
                "rows": row.output.get("rows", []),
            }

        if row.requires_approval and row.status != "approved":
            row.status = "approved"
            row.approved_at = datetime.utcnow()
            db.add(row)
            db.commit()

        _connection_id, creds = load_active_connection_credentials(db, tenant_id=tenant_id)
        if not self.settings.bigquery_mock_mode and creds is None:
            return {"status": "blocked", "reasons": ["No active BigQuery connection for tenant"]}
        result = self.connector.execute_query(row.sql, service_account_json=creds)
        actual_bytes = int(result.get("actual_bytes", row.estimated_bytes))
        self.cost.record(tenant_id, actual_bytes)

        row.actual_bytes = actual_bytes
        row.output = {"rows": result.get("rows", [])}
        row.status = "executed"
        row.executed_at = datetime.utcnow()
        db.add(row)
        db.commit()
        db.refresh(row)
        return {
            "status": "executed",
            "preview_id": row.id,
            "estimated_bytes": row.estimated_bytes,
            "actual_bytes": row.actual_bytes,
            "rows": row.output.get("rows", []),
        }
