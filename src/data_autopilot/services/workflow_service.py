from __future__ import annotations

from datetime import datetime, timedelta
from hashlib import sha256
import json
from uuid import uuid4

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from data_autopilot.config.settings import get_settings
from data_autopilot.models.entities import (
    CatalogColumn,
    CatalogTable,
    WorkflowRun,
    WorkflowStep,
)
from data_autopilot.services.bigquery_connector import BigQueryConnector
from data_autopilot.services.connection_context import load_active_connection_credentials
from data_autopilot.services.dashboard_service import DashboardService
from data_autopilot.services.memo_service import MemoService


class WorkflowService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.connector = BigQueryConnector()
        self.dashboard = DashboardService()
        self.memo = MemoService()

    def _hash(self, payload: dict) -> str:
        return sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()

    def _upsert_step(
        self,
        db: Session,
        workflow_id: str,
        step_name: str,
        payload: dict,
        output: dict,
        status: str = "success",
        retry_count: int = 0,
        error: str | None = None,
    ) -> WorkflowStep:
        input_hash = self._hash(payload)
        stmt = select(WorkflowStep).where(
            WorkflowStep.workflow_id == workflow_id,
            WorkflowStep.step_name == step_name,
            WorkflowStep.input_hash == input_hash,
            WorkflowStep.status == "success",
        )
        existing = db.execute(stmt).scalar_one_or_none()
        if existing is not None:
            return existing

        now = datetime.utcnow()
        step = WorkflowStep(
            workflow_id=workflow_id,
            step_name=step_name,
            status=status,
            output=output,
            input_hash=input_hash,
            output_hash=self._hash(output),
            retry_count=retry_count,
            error=error,
            started_at=now,
            finished_at=now,
        )
        db.add(step)
        db.commit()
        db.refresh(step)
        return step

    def start(self, db: Session, tenant_id: str, workflow_type: str) -> WorkflowRun:
        run = WorkflowRun(
            id=f"wf_{uuid4().hex[:12]}",
            tenant_id=tenant_id,
            workflow_type=workflow_type,
            status="running",
            started_at=datetime.utcnow(),
        )
        db.add(run)
        db.commit()
        db.refresh(run)
        return run

    def _active_count(self, db: Session, tenant_id: str, workflow_type: str | None = None) -> int:
        stale_cutoff = datetime.utcnow() - timedelta(minutes=30)
        stale_runs = db.execute(
            select(WorkflowRun).where(
                WorkflowRun.tenant_id == tenant_id,
                WorkflowRun.status == "running",
                WorkflowRun.started_at < stale_cutoff,
            )
        ).scalars().all()
        for run in stale_runs:
            run.status = "failed"
            run.finished_at = datetime.utcnow()
            db.add(run)
        if stale_runs:
            db.commit()

        stmt = select(WorkflowRun).where(WorkflowRun.tenant_id == tenant_id, WorkflowRun.status == "running")
        if workflow_type:
            stmt = stmt.where(WorkflowRun.workflow_type == workflow_type)
        return len(db.execute(stmt).scalars().all())

    def has_capacity(self, db: Session, tenant_id: str, workflow_type: str) -> bool:
        if workflow_type == "profile":
            active_profile = self._active_count(db, tenant_id=tenant_id, workflow_type="profile")
            active_total = self._active_count(db, tenant_id=tenant_id)
            return active_profile < self.settings.per_org_max_profile_workflows and active_total < self.settings.per_org_max_workflows
        active = self._active_count(db, tenant_id=tenant_id)
        return active < self.settings.per_org_max_workflows

    def _resume_or_start(self, db: Session, tenant_id: str, workflow_type: str, workflow_id: str | None = None) -> WorkflowRun:
        if workflow_id:
            existing = db.execute(select(WorkflowRun).where(WorkflowRun.id == workflow_id)).scalar_one_or_none()
            if existing is not None and existing.tenant_id == tenant_id and existing.workflow_type == workflow_type:
                if existing.status in {"failed", "partial_failure"}:
                    existing.status = "running"
                    existing.finished_at = None
                    db.add(existing)
                    db.commit()
                    db.refresh(existing)
                return existing

        resumable = db.execute(
            select(WorkflowRun)
            .where(
                WorkflowRun.tenant_id == tenant_id,
                WorkflowRun.workflow_type == workflow_type,
                WorkflowRun.status.in_(["failed", "partial_failure"]),
            )
            .order_by(WorkflowRun.started_at.desc())
        ).scalar_one_or_none()
        if resumable is not None:
            resumable.status = "running"
            resumable.finished_at = None
            db.add(resumable)
            db.commit()
            db.refresh(resumable)
            return resumable

        return self.start(db, tenant_id, workflow_type)

    def _existing_success_step(self, db: Session, workflow_id: str, step_name: str, payload: dict) -> WorkflowStep | None:
        input_hash = self._hash(payload)
        return db.execute(
            select(WorkflowStep).where(
                WorkflowStep.workflow_id == workflow_id,
                WorkflowStep.step_name == step_name,
                WorkflowStep.input_hash == input_hash,
                WorkflowStep.status == "success",
            )
        ).scalar_one_or_none()

    def _maybe_fail(self, step_name: str, payload: dict) -> None:
        schedule = payload.get("failure_modes", {})
        if step_name not in schedule:
            return
        conf = schedule.get(step_name)
        if isinstance(conf, str):
            raise RuntimeError(conf)
        if not isinstance(conf, dict):
            return
        mode = str(conf.get("mode", "transient_error"))
        remaining = int(conf.get("remaining", 0))
        if payload.get("sampling") and mode == "timeout":
            return
        if remaining > 0:
            conf["remaining"] = remaining - 1
            raise RuntimeError(mode)

    def _mark_skipped_step(self, db: Session, run: WorkflowRun, step_name: str, payload: dict, error: str) -> None:
        now = datetime.utcnow()
        step = WorkflowStep(
            workflow_id=run.id,
            step_name=step_name,
            status="skipped",
            output={"skipped": True, "reason": error},
            input_hash=self._hash(payload),
            output_hash=self._hash({"skipped": True, "reason": error}),
            retry_count=0,
            error=error,
            started_at=now,
            finished_at=now,
        )
        db.add(step)
        db.commit()

    def _execute_step(
        self,
        db: Session,
        run: WorkflowRun,
        step_name: str,
        payload: dict,
        execute_fn,
    ) -> dict:
        existing = self._existing_success_step(db, run.id, step_name, payload)
        if existing is not None:
            return dict(existing.output or {})

        retries = 0
        max_retries = 3
        retryable = {"transient_error", "timeout"}
        while True:
            started = datetime.utcnow()
            try:
                output = execute_fn()
                ended = datetime.utcnow()
                self._upsert_step(
                    db,
                    run.id,
                    step_name,
                    payload,
                    output,
                    status="success",
                    retry_count=retries,
                )
                return output
            except RuntimeError as exc:
                mode = str(exc)
                if mode in retryable and retries < max_retries:
                    retries += 1
                    continue

                ended = datetime.utcnow()
                step = WorkflowStep(
                    workflow_id=run.id,
                    step_name=step_name,
                    status="failed",
                    output={},
                    input_hash=self._hash(payload),
                    output_hash=self._hash({}),
                    retry_count=retries,
                    error=mode,
                    started_at=started,
                    finished_at=ended,
                )
                db.add(step)
                db.commit()
                raise

    def _partial_failure_response(self, db: Session, run: WorkflowRun, step_name: str, error: str, retry_count: int) -> dict:
        completed = db.execute(
            select(WorkflowStep)
            .where(WorkflowStep.workflow_id == run.id, WorkflowStep.status == "success")
            .order_by(WorkflowStep.id.asc())
        ).scalars().all()
        return {
            "workflow_id": run.id,
            "workflow_status": "partial_failure",
            "completed_steps": [
                {"step": s.step_name, "status": s.status, "output_summary": str(s.output)[:180]}
                for s in completed
            ],
            "failed_step": {"step": step_name, "error": error, "retry_count": retry_count},
            "available_actions": [
                {"action": "retry", "description": "Retry"},
                {"action": "retry_with_sampling", "description": "Retry with sampling"},
                {"action": "skip_and_continue", "description": "Skip this step and continue"},
            ],
        }

    def finish(self, db: Session, run: WorkflowRun, status: str = "success") -> WorkflowRun:
        run.status = status
        run.finished_at = datetime.utcnow()
        db.add(run)
        db.commit()
        db.refresh(run)
        return run

    def list_runs(self, db: Session, tenant_id: str, status: str | None = None, workflow_type: str | None = None) -> list[WorkflowRun]:
        stmt = select(WorkflowRun).where(WorkflowRun.tenant_id == tenant_id).order_by(WorkflowRun.started_at.desc())
        if status:
            stmt = stmt.where(WorkflowRun.status == status)
        if workflow_type:
            stmt = stmt.where(WorkflowRun.workflow_type == workflow_type)
        return list(db.execute(stmt).scalars().all())

    def cancel_run(self, db: Session, tenant_id: str, workflow_id: str) -> WorkflowRun | None:
        row = db.execute(
            select(WorkflowRun).where(WorkflowRun.id == workflow_id, WorkflowRun.tenant_id == tenant_id)
        ).scalar_one_or_none()
        if row is None:
            return None
        if row.status in {"success", "failed", "cancelled"}:
            return row
        row.status = "cancelled"
        row.finished_at = datetime.utcnow()
        db.add(row)
        db.commit()
        db.refresh(row)
        return row

    def run_profile_flow(self, db: Session, tenant_id: str, payload: dict | None = None, workflow_id: str | None = None) -> dict:
        flow_payload = payload or {}
        run = self._resume_or_start(db, tenant_id, "profile", workflow_id=workflow_id)
        connection_id, creds = load_active_connection_credentials(db, tenant_id=tenant_id)
        if connection_id is None:
            connection_id = f"conn_{tenant_id}"
        if not self.settings.bigquery_mock_mode and creds is None:
            self.finish(db, run, status="partial_failure")
            return self._partial_failure_response(db, run, "introspect_schemas", "missing_connection_credentials", 0)
        try:
            schema = self._execute_step(
                db,
                run,
                "introspect_schemas",
                {"tenant_id": tenant_id, "connection_id": connection_id},
                lambda: (self._maybe_fail("introspect_schemas", flow_payload), self.connector.introspect(connection_id=connection_id, service_account_json=creds))[1],
            )
        except RuntimeError as exc:
            self.finish(db, run, status="partial_failure")
            return self._partial_failure_response(db, run, "introspect_schemas", str(exc), 3)
        datasets = schema["datasets"]
        table_count = sum(len(ds["tables"]) for ds in datasets.values())
        self._upsert_step(db, run.id, "introspect_schemas_summary", {"connection_id": connection_id}, {"datasets": len(datasets), "tables": table_count, "cache_hit": schema.get("cache_hit", False)})

        db.execute(delete(CatalogTable).where(CatalogTable.tenant_id == tenant_id))
        db.execute(delete(CatalogColumn).where(CatalogColumn.tenant_id == tenant_id))
        db.commit()

        profiled_columns = 0
        for dataset, ds_payload in datasets.items():
            for table_name, table_payload in ds_payload["tables"].items():
                row_count_est = int(table_payload.get("row_count_est", 0) or 0)
                bytes_est = int(table_payload.get("bytes_est", 0) or 0)
                freshness_hours = int(table_payload.get("freshness_hours", 0) or 0)
                table = CatalogTable(
                    tenant_id=tenant_id,
                    connection_id=connection_id,
                    dataset=dataset,
                    table_name=table_name,
                    row_count_est=row_count_est,
                    bytes_est=bytes_est,
                    freshness_hours=freshness_hours,
                )
                db.add(table)

                for col in table_payload["columns"]:
                    col_name = str(col["name"]).lower()
                    pii_conf = 95 if any(token in col_name for token in {"email", "phone", "ssn"}) else 20
                    db.add(
                        CatalogColumn(
                            tenant_id=tenant_id,
                            connection_id=connection_id,
                            dataset=dataset,
                            table_name=table_name,
                            column_name=col["name"],
                            data_type=col["type"],
                            null_pct=0 if col_name.endswith("_id") else 5,
                            distinct_est=max(1000, min(1_000_000, row_count_est // 4 if row_count_est else 1000)),
                            is_pii=pii_conf >= 80,
                            pii_confidence=pii_conf,
                        )
                    )
                    profiled_columns += 1
        db.commit()

        try:
            self._execute_step(
                db,
                run,
                "profile_tables",
                {"sample": "dynamic"},
                lambda: (self._maybe_fail("profile_tables", flow_payload), {"profiled_tables": table_count})[1],
            )
            self._execute_step(
                db,
                run,
                "profile_columns",
                {"sample": "dynamic"},
                lambda: (self._maybe_fail("profile_columns", flow_payload), {"profiled_columns": profiled_columns})[1],
            )
        except RuntimeError as exc:
            if flow_payload.get("skip_on_error"):
                self._mark_skipped_step(db, run, "profile_columns", {"sample": "dynamic"}, str(exc))
            else:
                self.finish(db, run, status="partial_failure")
                return self._partial_failure_response(db, run, "profile_columns", str(exc), 3)
        
        try:
            self._execute_step(
                db,
                run,
                "detect_pii",
                {},
                lambda: (self._maybe_fail("detect_pii", flow_payload), {"high_confidence": ["email"], "low_confidence": []})[1],
            )
        except RuntimeError as exc:
            if flow_payload.get("skip_on_error"):
                self._mark_skipped_step(db, run, "detect_pii", {}, str(exc))
            else:
                self.finish(db, run, status="partial_failure")
                return self._partial_failure_response(db, run, "detect_pii", str(exc), 3)

        recommended: list[str] = []
        for dataset, ds_payload in datasets.items():
            for table_name in ds_payload["tables"].keys():
                full = f"{dataset}.{table_name}"
                name = table_name.lower()
                if any(token in name for token in {"user", "account", "member"}):
                    recommended.append(full)
                elif any(token in name for token in {"event", "click", "activity"}):
                    recommended.append(full)
                elif any(token in name for token in {"order", "transaction", "payment", "revenue"}):
                    recommended.append(full)
        if not recommended:
            for dataset, ds_payload in datasets.items():
                for table_name in ds_payload["tables"].keys():
                    recommended.append(f"{dataset}.{table_name}")
                    if len(recommended) >= 3:
                        break
                if len(recommended) >= 3:
                    break
        try:
            self._execute_step(db, run, "recommend_starters", {}, lambda: {"recommended": recommended})
            self._execute_step(db, run, "store_catalog", {}, lambda: {"stored": True, "connection_id": connection_id})
        except RuntimeError as exc:
            if flow_payload.get("skip_on_error"):
                self._mark_skipped_step(db, run, "store_catalog", {}, str(exc))
            else:
                self.finish(db, run, status="partial_failure")
                return self._partial_failure_response(db, run, "store_catalog", str(exc), 3)

        self.finish(db, run)
        return {
            "workflow_id": run.id,
            "status": run.status,
            "connection_id": connection_id,
            "resumed": workflow_id is not None,
            "sampling_mode": bool(flow_payload.get("sampling")),
            "skip_on_error": bool(flow_payload.get("skip_on_error")),
        }

    def run_dashboard_flow(self, db: Session, tenant_id: str, payload: dict | None = None, workflow_id: str | None = None) -> dict:
        flow_payload = payload or {}
        run = self._resume_or_start(db, tenant_id, "dashboard", workflow_id=workflow_id)
        tables = db.execute(select(CatalogTable).where(CatalogTable.tenant_id == tenant_id)).scalars().all()
        table_names = [f"{t.dataset}.{t.table_name}" for t in tables]
        self._execute_step(db, run, "load_catalog", {}, lambda: {"tables": table_names})
        includes_revenue = any(t.table_name == "orders" for t in tables)
        self._execute_step(db, run, "select_template", {}, lambda: {"templates": ["exec_overview", "data_health"] + (["revenue_volume"] if includes_revenue else [])})
        try:
            result = self._execute_step(
                db,
                run,
                "store_artifact",
                {},
                lambda: (self._maybe_fail("store_artifact", flow_payload), self.dashboard.generate(db, tenant_id=tenant_id))[1],
            )
        except RuntimeError as exc:
            self.finish(db, run, status="partial_failure")
            return self._partial_failure_response(db, run, "store_artifact", str(exc), 3)
        self.finish(db, run)
        return {"workflow_id": run.id, "status": run.status, **result, "resumed": workflow_id is not None}

    def run_memo_flow(self, db: Session, tenant_id: str, payload: dict | None = None, workflow_id: str | None = None) -> dict:
        flow_payload = payload or {}
        run = self._resume_or_start(db, tenant_id, "memo", workflow_id=workflow_id)
        try:
            result = self._execute_step(
                db,
                run,
                "store_artifact",
                {},
                lambda: (self._maybe_fail("store_artifact", flow_payload), self.memo.generate(db, tenant_id=tenant_id))[1],
            )
        except RuntimeError as exc:
            self.finish(db, run, status="partial_failure")
            return self._partial_failure_response(db, run, "store_artifact", str(exc), 3)
        self.finish(db, run)
        return {"workflow_id": run.id, "status": run.status, **result, "resumed": workflow_id is not None}
