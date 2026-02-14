from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from data_autopilot.config.settings import get_settings
from data_autopilot.db.session import get_db
from data_autopilot.models.entities import Role
from data_autopilot.security.rbac import require_admin, require_member_or_admin, role_from_headers
from data_autopilot.security.tenancy import ensure_tenant_scope, tenant_from_headers
from data_autopilot.schemas.common import (
    AgentRequest,
    AgentResponse,
    ConnectorRequest,
    ConnectorResponse,
    FeedbackRequest,
    FeedbackResponse,
    HealthResponse,
)
from data_autopilot.services.agent_service import AgentService
from data_autopilot.services.artifact_service import ArtifactService
from data_autopilot.services.audit import AuditService
from data_autopilot.services.connector_service import ConnectorService
from data_autopilot.services.feedback_service import FeedbackService
from data_autopilot.services.metabase_client import MetabaseClient
from data_autopilot.services.workflow_service import WorkflowService
from data_autopilot.services.bigquery_connector import BigQueryConnector
from data_autopilot.services.degradation_service import DegradationService
from data_autopilot.services.query_service import QueryService
from data_autopilot.services.alert_service import AlertService
from data_autopilot.services.notification_service import NotificationService
from data_autopilot.services.tenant_admin_service import TenantAdminService
from data_autopilot.models.entities import AlertSeverity, AlertStatus
from data_autopilot.models.entities import WorkflowQueue
from data_autopilot.models.entities import CatalogColumn

router = APIRouter()
agent_service = AgentService()
feedback_service = FeedbackService()
workflow_service = WorkflowService()
connector_service = ConnectorService()
metabase_client = MetabaseClient()
bigquery_connector = BigQueryConnector()
degradation_service = DegradationService()
artifact_service = ArtifactService()
audit_service = AuditService()
query_service = QueryService()
alert_service = AlertService()
notification_service = NotificationService()
tenant_admin_service = TenantAdminService()


def _auto_alert_from_workflow_result(db: Session, org_id: str, workflow_type: str, result: dict) -> None:
    if result.get("workflow_status") != "partial_failure":
        return
    failed = result.get("failed_step", {})
    failed_step = str(failed.get("step", "unknown_step"))
    message = str(failed.get("error", "workflow step failed"))
    alert_service.create_or_update(
        db,
        tenant_id=org_id,
        dedupe_key=f"workflow_partial_failure:{workflow_type}:{failed_step}",
        title=f"{workflow_type} workflow partial failure",
        message=f"Step '{failed_step}' failed: {message}",
        severity=AlertSeverity.P1,
        source_type="workflow",
        source_id=workflow_type,
    )


def _auto_alert_from_memo_anomalies(db: Session, org_id: str, artifact_id: str) -> None:
    artifact = artifact_service.get(db, artifact_id=artifact_id, tenant_id=org_id)
    if artifact is None:
        return
    packet = (artifact.data or {}).get("packet", {})
    notes = packet.get("anomaly_notes", [])
    if not isinstance(notes, list) or not notes:
        return
    for note in notes:
        key = str(note).strip()
        if not key:
            continue
        alert_service.create_or_update(
            db,
            tenant_id=org_id,
            dedupe_key=f"memo_anomaly:{abs(hash(key))}",
            title="Data quality anomaly detected",
            message=key,
            severity=AlertSeverity.P2,
            source_type="data_quality",
            source_id="memo",
        )


@router.get('/health', response_model=HealthResponse)
def health() -> HealthResponse:
    settings = get_settings()
    return HealthResponse(status="ok", app=settings.app_name)


@router.get('/ready')
def ready() -> dict:
    settings = get_settings()
    checks: dict[str, dict] = {}

    if settings.bigquery_mock_mode:
        checks["bigquery"] = {"ok": True, "mode": "mock"}
    else:
        checks["bigquery"] = bigquery_connector.test_connection()

    if settings.metabase_mock_mode:
        checks["metabase"] = {"ok": True, "mode": "mock"}
    else:
        checks["metabase"] = metabase_client.test_connection()

    ok = all(bool(v.get("ok")) for v in checks.values())
    return {"ok": ok, "checks": checks}


@router.post('/api/v1/agent/run', response_model=AgentResponse)
def run_agent(
    req: AgentRequest,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> AgentResponse:
    ensure_tenant_scope(tenant_id, req.org_id)
    require_member_or_admin(role)
    settings = get_settings()
    if settings.allow_real_query_execution:
        raise HTTPException(status_code=500, detail="Real query execution is not wired yet")

    result = agent_service.run(db=db, org_id=req.org_id, user_id=req.user_id, message=req.message)
    audit_service.log(
        db,
        tenant_id=req.org_id,
        event_type="agent_run",
        payload={"user_id": req.user_id, "session_id": req.session_id, "response_type": result.get("response_type")},
    )
    return AgentResponse(**result)


@router.post('/api/v1/feedback', response_model=FeedbackResponse)
def create_feedback(
    req: FeedbackRequest,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> FeedbackResponse:
    ensure_tenant_scope(tenant_id, req.tenant_id)
    row = feedback_service.create(db, req)
    audit_service.log(
        db,
        tenant_id=req.tenant_id,
        event_type="feedback_created",
        payload={
            "artifact_id": req.artifact_id,
            "artifact_version": req.artifact_version,
            "artifact_type": req.artifact_type,
            "feedback_type": req.feedback_type,
        },
    )
    return FeedbackResponse(id=row.id, created_at=row.created_at)


@router.get('/api/v1/feedback/summary')
def feedback_summary(
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    summary = feedback_service.summary(db, tenant_id=org_id)
    audit_service.log(db, tenant_id=org_id, event_type="feedback_summary_viewed", payload={"org_id": org_id})
    return summary


@router.post('/api/v1/workflows/profile')
def run_profile_workflow(
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    require_member_or_admin(role)
    if not workflow_service.has_capacity(db, tenant_id=org_id, workflow_type="profile"):
        queued = degradation_service.enqueue(
            db,
            tenant_id=org_id,
            workflow_type="profile",
            payload={"org_id": org_id},
            reason="concurrency_limit",
        )
        audit_service.log(db, tenant_id=org_id, event_type="workflow_queued", payload={"workflow_type": "profile", "reason": "concurrency_limit", "queue_id": queued.get("queue_id")})
        return queued
    if not degradation_service.warehouse_available():
        queued = degradation_service.enqueue(
            db,
            tenant_id=org_id,
            workflow_type="profile",
            payload={"org_id": org_id},
            reason="warehouse_unavailable",
        )
        audit_service.log(db, tenant_id=org_id, event_type="workflow_queued", payload={"workflow_type": "profile", "reason": "warehouse_unavailable"})
        return queued
    result = workflow_service.run_profile_flow(db, tenant_id=org_id)
    _auto_alert_from_workflow_result(db, org_id=org_id, workflow_type="profile", result=result)
    audit_service.log(db, tenant_id=org_id, event_type="workflow_run", payload={"workflow_type": "profile", "status": result.get("status")})
    return result


@router.post('/api/v1/workflows/dashboard')
def run_dashboard_workflow(
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    require_member_or_admin(role)
    if not workflow_service.has_capacity(db, tenant_id=org_id, workflow_type="dashboard"):
        queued = degradation_service.enqueue(
            db,
            tenant_id=org_id,
            workflow_type="dashboard",
            payload={"org_id": org_id},
            reason="concurrency_limit",
        )
        audit_service.log(db, tenant_id=org_id, event_type="workflow_queued", payload={"workflow_type": "dashboard", "reason": "concurrency_limit", "queue_id": queued.get("queue_id")})
        return queued
    if not degradation_service.warehouse_available():
        queued = degradation_service.enqueue(
            db,
            tenant_id=org_id,
            workflow_type="dashboard",
            payload={"org_id": org_id},
            reason="warehouse_unavailable",
        )
        audit_service.log(db, tenant_id=org_id, event_type="workflow_queued", payload={"workflow_type": "dashboard", "reason": "warehouse_unavailable"})
        return queued
    result = workflow_service.run_dashboard_flow(db, tenant_id=org_id)
    _auto_alert_from_workflow_result(db, org_id=org_id, workflow_type="dashboard", result=result)
    audit_service.log(db, tenant_id=org_id, event_type="workflow_run", payload={"workflow_type": "dashboard", "status": result.get("status"), "artifact_id": result.get("artifact_id")})
    return result


@router.post('/api/v1/workflows/memo')
def run_memo_workflow(
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    require_member_or_admin(role)
    if not workflow_service.has_capacity(db, tenant_id=org_id, workflow_type="memo"):
        queued = degradation_service.enqueue(
            db,
            tenant_id=org_id,
            workflow_type="memo",
            payload={"org_id": org_id},
            reason="concurrency_limit",
        )
        audit_service.log(db, tenant_id=org_id, event_type="workflow_queued", payload={"workflow_type": "memo", "reason": "concurrency_limit", "queue_id": queued.get("queue_id")})
        return queued
    if not degradation_service.llm_available():
        queued = degradation_service.enqueue(
            db,
            tenant_id=org_id,
            workflow_type="memo",
            payload={"org_id": org_id},
            reason="llm_unavailable",
        )
        audit_service.log(db, tenant_id=org_id, event_type="workflow_queued", payload={"workflow_type": "memo", "reason": "llm_unavailable"})
        return queued
    result = workflow_service.run_memo_flow(db, tenant_id=org_id)
    _auto_alert_from_workflow_result(db, org_id=org_id, workflow_type="memo", result=result)
    if result.get("status") == "success" and result.get("artifact_id"):
        _auto_alert_from_memo_anomalies(db, org_id=org_id, artifact_id=str(result["artifact_id"]))
    audit_service.log(db, tenant_id=org_id, event_type="workflow_run", payload={"workflow_type": "memo", "status": result.get("status"), "artifact_id": result.get("artifact_id")})
    return result


@router.post('/api/v1/workflows/process-queue')
def process_queue(
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    require_member_or_admin(role)
    queued = degradation_service.fetch_queued(db, tenant_id=org_id)
    available_slots = max(0, get_settings().per_org_max_workflows - workflow_service._active_count(db, tenant_id=org_id))
    processed = 0
    skipped = 0
    dead_lettered = 0
    deferred_due_capacity = 0
    for row in queued:
        if processed >= available_slots:
            deferred_due_capacity += 1
            continue
        if row.workflow_type in {"profile", "dashboard"} and not degradation_service.warehouse_available():
            skipped += 1
            continue
        if row.workflow_type == "memo" and not degradation_service.llm_available():
            skipped += 1
            continue

        try:
            payload = dict(row.payload or {})
            if row.workflow_type == "profile":
                result = workflow_service.run_profile_flow(db, tenant_id=row.tenant_id, payload=payload)
            elif row.workflow_type == "dashboard":
                result = workflow_service.run_dashboard_flow(db, tenant_id=row.tenant_id, payload=payload)
            else:
                result = workflow_service.run_memo_flow(db, tenant_id=row.tenant_id, payload=payload)

            if result.get("workflow_status") == "partial_failure":
                degradation_service.mark_failed_attempt(db, row, result.get("failed_step", {}).get("error", "workflow_failed"))
                row.payload = payload
                db.add(row)
                db.commit()
                if int(row.attempts or 0) >= 3:
                    steps = result.get("completed_steps", [])
                    degradation_service.move_to_dead_letter(db, row, step_states=steps)
                    alert_service.create_or_update(
                        db,
                        tenant_id=org_id,
                        dedupe_key=f"workflow_dead_letter:{row.workflow_type}:{row.id}",
                        title=f"{row.workflow_type} workflow moved to dead letter",
                        message=f"Queue item {row.id} failed repeatedly and was moved to dead letter.",
                        severity=AlertSeverity.P0,
                        source_type="workflow_queue",
                        source_id=row.id,
                    )
                    dead_lettered += 1
                else:
                    skipped += 1
                continue

            degradation_service.mark_processed(db, row)
            processed += 1
        except Exception as exc:
            degradation_service.mark_failed_attempt(db, row, str(exc))
            if int(row.attempts or 0) >= 3:
                degradation_service.move_to_dead_letter(db, row, step_states=[])
                alert_service.create_or_update(
                    db,
                    tenant_id=org_id,
                    dedupe_key=f"workflow_dead_letter:{row.workflow_type}:{row.id}",
                    title=f"{row.workflow_type} workflow moved to dead letter",
                    message=f"Queue item {row.id} failed repeatedly and was moved to dead letter.",
                    severity=AlertSeverity.P0,
                    source_type="workflow_queue",
                    source_id=row.id,
                )
                dead_lettered += 1
            else:
                skipped += 1

    payload = {
        "processed": processed,
        "skipped": skipped,
        "dead_lettered": dead_lettered,
        "deferred_due_capacity": deferred_due_capacity,
        "queued_total": len(queued),
    }
    audit_service.log(db, tenant_id=org_id, event_type="queue_processed", payload=payload)
    return payload


@router.get('/api/v1/workflows/queue')
def queue_status(
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    require_member_or_admin(role)
    queued = degradation_service.fetch_queued(db, tenant_id=org_id)
    items = []
    for idx, row in enumerate(queued, start=1):
        items.append(
            {
                "queue_id": row.id,
                "workflow_type": row.workflow_type,
                "reason": row.reason,
                "attempts": row.attempts,
                "position": idx,
                "created_at": row.created_at.isoformat(),
            }
        )
    response = {"org_id": org_id, "queued_total": len(items), "items": items}
    audit_service.log(db, tenant_id=org_id, event_type="queue_viewed", payload={"queued_total": len(items)})
    return response


@router.get('/api/v1/workflows/dead-letters')
def dead_letters(
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    require_member_or_admin(role)
    rows = degradation_service.fetch_dead_letters(db, tenant_id=org_id)
    response = {
        "org_id": org_id,
        "items": [
            {
                "dead_letter_id": row.id,
                "queue_id": row.queue_id,
                "workflow_type": row.workflow_type,
                "error_history": row.error_history,
                "created_at": row.created_at.isoformat(),
            }
            for row in rows
        ],
    }
    audit_service.log(db, tenant_id=org_id, event_type="dead_letters_viewed", payload={"count": len(response["items"])})
    return response


@router.post('/api/v1/workflows/retry')
def retry_workflow(
    org_id: str,
    workflow_type: str,
    workflow_id: str | None = None,
    action: str = "retry",
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    require_member_or_admin(role)
    payload: dict = {"retry_action": action}
    if action == "retry_with_sampling":
        payload["sampling"] = True
    elif action == "skip_and_continue":
        payload["skip_on_error"] = True

    if workflow_type == "profile":
        result = workflow_service.run_profile_flow(db, tenant_id=org_id, payload=payload, workflow_id=workflow_id)
    elif workflow_type == "dashboard":
        result = workflow_service.run_dashboard_flow(db, tenant_id=org_id, payload=payload, workflow_id=workflow_id)
    elif workflow_type == "memo":
        result = workflow_service.run_memo_flow(db, tenant_id=org_id, payload=payload, workflow_id=workflow_id)
    else:
        raise HTTPException(status_code=400, detail="Unsupported workflow_type")

    if result.get("status") == "success" and workflow_id:
        queued_rows = db.execute(
            select(WorkflowQueue).where(
                WorkflowQueue.tenant_id == org_id,
                WorkflowQueue.workflow_type == workflow_type,
                WorkflowQueue.status == "queued",
            )
        ).scalars().all()
        for row in queued_rows:
            degradation_service.mark_processed(db, row)

    audit_service.log(
        db,
        tenant_id=org_id,
        event_type="workflow_retry_requested",
        payload={"workflow_type": workflow_type, "workflow_id": workflow_id, "action": action, "result_status": result.get("status", result.get("workflow_status"))},
    )
    return result


@router.get('/api/v1/workflows/runs')
def list_workflow_runs(
    org_id: str,
    status: str | None = None,
    workflow_type: str | None = None,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    require_member_or_admin(role)
    rows = workflow_service.list_runs(db, tenant_id=org_id, status=status, workflow_type=workflow_type)
    items = [
        {
            "workflow_id": r.id,
            "workflow_type": r.workflow_type,
            "status": r.status,
            "started_at": r.started_at.isoformat(),
            "finished_at": r.finished_at.isoformat() if r.finished_at else None,
        }
        for r in rows
    ]
    audit_service.log(db, tenant_id=org_id, event_type="workflow_runs_listed", payload={"count": len(items), "status": status, "workflow_type": workflow_type})
    return {"org_id": org_id, "items": items}


@router.post('/api/v1/workflows/{workflow_id}/cancel')
def cancel_workflow_run(
    workflow_id: str,
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    require_member_or_admin(role)
    row = workflow_service.cancel_run(db, tenant_id=org_id, workflow_id=workflow_id)
    if row is None:
        raise HTTPException(status_code=404, detail="workflow not found")
    audit_service.log(db, tenant_id=org_id, event_type="workflow_run_cancelled", payload={"workflow_id": workflow_id, "status": row.status})
    return {
        "workflow_id": row.id,
        "workflow_type": row.workflow_type,
        "status": row.status,
        "finished_at": row.finished_at.isoformat() if row.finished_at else None,
    }


@router.get('/api/v1/artifacts')
def list_artifacts(
    org_id: str,
    artifact_type: str | None = None,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    from data_autopilot.models.entities import ArtifactType

    parsed_type = ArtifactType(artifact_type) if artifact_type else None
    rows = artifact_service.list_for_tenant(db, tenant_id=org_id, artifact_type=parsed_type)
    response = {
        "items": [
            {
                "artifact_id": r.id,
                "type": r.type.value,
                "version": r.version,
                "stale": r.stale,
                "created_at": r.created_at.isoformat(),
            }
            for r in rows
        ]
    }
    audit_service.log(db, tenant_id=org_id, event_type="artifacts_listed", payload={"count": len(response["items"])})
    return response


@router.get('/api/v1/artifacts/{artifact_id}')
def get_artifact(
    artifact_id: str,
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    row = artifact_service.get(db, artifact_id=artifact_id, tenant_id=org_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Artifact not found")
    response = {
        "artifact_id": row.id,
        "tenant_id": row.tenant_id,
        "type": row.type.value,
        "version": row.version,
        "stale": row.stale,
        "query_hashes": row.query_hashes,
        "data": row.data,
        "created_at": row.created_at.isoformat(),
    }
    audit_service.log(db, tenant_id=org_id, event_type="artifact_viewed", payload={"artifact_id": row.id, "version": row.version})
    return response


@router.get('/api/v1/artifacts/{artifact_id}/versions')
def artifact_versions(
    artifact_id: str,
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    versions = artifact_service.versions(db, artifact_id=artifact_id, tenant_id=org_id)
    response = {
        "artifact_id": artifact_id,
        "items": [
            {
                "version": v.version,
                "created_at": v.created_at.isoformat(),
                "query_hashes": v.query_hashes,
                "data": v.data,
            }
            for v in versions
        ],
    }
    audit_service.log(db, tenant_id=org_id, event_type="artifact_versions_viewed", payload={"artifact_id": artifact_id, "count": len(response["items"])})
    return response


@router.get('/api/v1/artifacts/{artifact_id}/lineage')
def artifact_lineage(
    artifact_id: str,
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    response = artifact_service.lineage(db, artifact_id=artifact_id, tenant_id=org_id)
    audit_service.log(db, tenant_id=org_id, event_type="artifact_lineage_viewed", payload={"artifact_id": artifact_id, "nodes": len(response.get("nodes", []))})
    return response


@router.get('/api/v1/artifacts/{artifact_id}/diff')
def artifact_diff(
    artifact_id: str,
    org_id: str,
    from_version: int | None = None,
    to_version: int | None = None,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    response = artifact_service.diff(db, artifact_id=artifact_id, tenant_id=org_id, from_version=from_version, to_version=to_version)
    audit_service.log(db, tenant_id=org_id, event_type="artifact_diff_viewed", payload={"artifact_id": artifact_id, "changes": len(response.get("changes", []))})
    return response


@router.get('/api/v1/memos/{artifact_id}/wow')
def memo_week_over_week(
    artifact_id: str,
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    response = artifact_service.memo_wow(db, artifact_id=artifact_id, tenant_id=org_id)
    audit_service.log(db, tenant_id=org_id, event_type="memo_wow_viewed", payload={"artifact_id": artifact_id, "rows": len(response.get("rows", []))})
    return response


@router.post('/api/v1/connectors/bigquery', response_model=ConnectorResponse)
def connect_bigquery(
    req: ConnectorRequest,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> ConnectorResponse:
    ensure_tenant_scope(tenant_id, req.org_id)
    require_admin(role)
    row = connector_service.connect(db, org_id=req.org_id, service_account_json=req.service_account_json)
    audit_service.log(db, tenant_id=req.org_id, event_type="connector_connect_requested", payload={"connection_id": row.id})
    return ConnectorResponse(connection_id=row.id, status=row.status)


@router.post('/api/v1/connectors/{connection_id}/disconnect')
def disconnect_bigquery(
    connection_id: str,
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    require_admin(role)
    response = connector_service.disconnect(db, org_id=org_id, connection_id=connection_id)
    audit_service.log(db, tenant_id=org_id, event_type="connector_disconnect_requested", payload={"connection_id": connection_id, "status": response.get("status")})
    return response


@router.get('/api/v1/pii/review')
def pii_review(
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    require_member_or_admin(role)
    rows = db.execute(select(CatalogColumn).where(CatalogColumn.tenant_id == org_id)).scalars().all()

    high: list[dict] = []
    low: list[dict] = []
    for col in rows:
        item = {
            "dataset": col.dataset,
            "table": col.table_name,
            "column": col.column_name,
            "confidence": col.pii_confidence,
            "is_pii": col.is_pii,
        }
        if col.pii_confidence >= 80:
            high.append(item)
        elif col.pii_confidence >= 30:
            low.append(item)

    response = {
        "org_id": org_id,
        "auto_tagged_high_confidence": high,
        "needs_review_low_confidence": low,
        "bulk_actions": ["confirm_all_high_confidence", "mark_not_pii_all_low_confidence"],
    }
    audit_service.log(
        db,
        tenant_id=org_id,
        event_type="pii_review_viewed",
        payload={"high_confidence_count": len(high), "low_confidence_count": len(low)},
    )
    return response


@router.post('/api/v1/pii/review/confirm')
def pii_review_confirm(
    org_id: str,
    decisions: list[dict],
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    require_member_or_admin(role)

    updated = 0
    for d in decisions:
        dataset = str(d.get("dataset", ""))
        table = str(d.get("table", ""))
        column = str(d.get("column", ""))
        is_pii = bool(d.get("is_pii", False))
        row = db.execute(
            select(CatalogColumn).where(
                CatalogColumn.tenant_id == org_id,
                CatalogColumn.dataset == dataset,
                CatalogColumn.table_name == table,
                CatalogColumn.column_name == column,
            )
        ).scalar_one_or_none()
        if row is None:
            continue
        row.is_pii = is_pii
        db.add(row)
        updated += 1
    db.commit()
    audit_service.log(db, tenant_id=org_id, event_type="pii_review_confirmed", payload={"updated": updated})
    return {"org_id": org_id, "updated": updated}


@router.post('/api/v1/queries/preview')
def query_preview(
    req: dict,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    org_id = str(req.get("org_id", ""))
    sql = str(req.get("sql", ""))
    ensure_tenant_scope(tenant_id, org_id)
    require_member_or_admin(role)
    if not sql.strip():
        raise HTTPException(status_code=400, detail="sql is required")
    result = query_service.preview(db, tenant_id=org_id, sql=sql)
    audit_service.log(
        db,
        tenant_id=org_id,
        event_type="query_previewed",
        payload={"status": result.get("status"), "requires_approval": result.get("requires_approval", False)},
    )
    return result


@router.post('/api/v1/queries/approve-run')
def query_approve_run(
    req: dict,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    org_id = str(req.get("org_id", ""))
    preview_id = str(req.get("preview_id", ""))
    ensure_tenant_scope(tenant_id, org_id)
    require_member_or_admin(role)
    if not preview_id:
        raise HTTPException(status_code=400, detail="preview_id is required")
    result = query_service.approve_and_run(db, tenant_id=org_id, preview_id=preview_id)
    if result.get("status") == "not_found":
        raise HTTPException(status_code=404, detail="preview_id not found")
    audit_service.log(
        db,
        tenant_id=org_id,
        event_type="query_approved_run",
        payload={"preview_id": preview_id, "status": result.get("status"), "actual_bytes": result.get("actual_bytes", 0)},
    )
    return result


@router.post('/api/v1/alerts')
def create_alert(
    req: dict,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    org_id = str(req.get("org_id", ""))
    ensure_tenant_scope(tenant_id, org_id)
    require_member_or_admin(role)

    dedupe_key = str(req.get("dedupe_key", ""))
    title = str(req.get("title", ""))
    message = str(req.get("message", ""))
    severity = str(req.get("severity", "P2"))
    source_type = str(req.get("source_type", "system"))
    source_id = req.get("source_id")

    if not dedupe_key or not title or not message:
        raise HTTPException(status_code=400, detail="dedupe_key, title, and message are required")

    row = alert_service.create_or_update(
        db,
        tenant_id=org_id,
        dedupe_key=dedupe_key,
        title=title,
        message=message,
        severity=AlertSeverity(severity),
        source_type=source_type,
        source_id=source_id,
    )
    notification_service.queue_for_alert(db, row, event_type="created")
    audit_service.log(
        db,
        tenant_id=org_id,
        event_type="alert_created_or_updated",
        payload={"alert_id": row.id, "severity": row.severity.value, "status": row.status.value},
    )
    return {
        "alert_id": row.id,
        "status": row.status.value,
        "severity": row.severity.value,
        "next_escalation_at": row.next_escalation_at.isoformat(),
        "escalated_count": row.escalated_count,
        "snoozed_until": row.snoozed_until.isoformat() if row.snoozed_until else None,
    }


@router.get('/api/v1/alerts')
def list_alerts(
    org_id: str,
    status: str | None = None,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    require_member_or_admin(role)
    parsed = AlertStatus(status) if status else None
    rows = alert_service.list_for_tenant(db, tenant_id=org_id, status=parsed)
    items = [
        {
            "alert_id": r.id,
            "title": r.title,
            "message": r.message,
            "severity": r.severity.value,
            "status": r.status.value,
            "escalated_count": r.escalated_count,
            "next_escalation_at": r.next_escalation_at.isoformat(),
            "acknowledged_by": r.acknowledged_by,
            "snoozed_until": r.snoozed_until.isoformat() if r.snoozed_until else None,
            "snoozed_by": r.snoozed_by,
            "snoozed_reason": r.snoozed_reason,
        }
        for r in rows
    ]
    audit_service.log(db, tenant_id=org_id, event_type="alerts_listed", payload={"count": len(items), "status": status})
    return {"org_id": org_id, "items": items}


@router.post('/api/v1/alerts/{alert_id}/ack')
def ack_alert(
    alert_id: str,
    req: dict,
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    require_member_or_admin(role)
    user_id = str(req.get("user_id", ""))
    if not user_id:
        raise HTTPException(status_code=400, detail="user_id is required")
    row = alert_service.acknowledge(db, tenant_id=org_id, alert_id=alert_id, user_id=user_id)
    if row is None:
        raise HTTPException(status_code=404, detail="alert not found")
    audit_service.log(db, tenant_id=org_id, event_type="alert_acknowledged", payload={"alert_id": row.id, "user_id": user_id})
    return {"alert_id": row.id, "status": row.status.value, "acknowledged_by": row.acknowledged_by}


@router.post('/api/v1/alerts/{alert_id}/snooze')
def snooze_alert(
    alert_id: str,
    req: dict,
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    require_member_or_admin(role)
    user_id = str(req.get("user_id", ""))
    duration_minutes = int(req.get("duration_minutes", 60))
    reason = req.get("reason")
    if not user_id:
        raise HTTPException(status_code=400, detail="user_id is required")
    row = alert_service.snooze(
        db,
        tenant_id=org_id,
        alert_id=alert_id,
        user_id=user_id,
        duration_minutes=duration_minutes,
        reason=str(reason) if reason is not None else None,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="alert not found")
    audit_service.log(
        db,
        tenant_id=org_id,
        event_type="alert_snoozed",
        payload={"alert_id": row.id, "user_id": user_id, "duration_minutes": duration_minutes},
    )
    return {
        "alert_id": row.id,
        "status": row.status.value,
        "snoozed_until": row.snoozed_until.isoformat() if row.snoozed_until else None,
        "snoozed_by": row.snoozed_by,
    }


@router.post('/api/v1/alerts/{alert_id}/resolve')
def resolve_alert(
    alert_id: str,
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    require_member_or_admin(role)
    row = alert_service.resolve(db, tenant_id=org_id, alert_id=alert_id)
    if row is None:
        raise HTTPException(status_code=404, detail="alert not found")
    audit_service.log(db, tenant_id=org_id, event_type="alert_resolved", payload={"alert_id": row.id})
    return {"alert_id": row.id, "status": row.status.value}


@router.post('/api/v1/alerts/escalate')
def escalate_alerts(
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    require_member_or_admin(role)
    rows = alert_service.escalate_due(db, tenant_id=org_id)
    for row in rows:
        notification_service.queue_for_alert(db, row, event_type="escalated")
    audit_service.log(db, tenant_id=org_id, event_type="alerts_escalated", payload={"count": len(rows)})
    return {"org_id": org_id, "escalated": len(rows), "alert_ids": [r.id for r in rows]}


@router.get('/api/v1/alerts/policy')
def get_alert_policy(
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    require_member_or_admin(role)
    policy = alert_service.get_policy(db, tenant_id=org_id)
    audit_service.log(db, tenant_id=org_id, event_type="alert_policy_viewed", payload={"policy": policy})
    return {"org_id": org_id, "policy": policy}


@router.post('/api/v1/alerts/policy')
def set_alert_policy(
    req: dict,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    org_id = str(req.get("org_id", ""))
    ensure_tenant_scope(tenant_id, org_id)
    require_admin(role)
    policy = req.get("policy", {})
    if not isinstance(policy, dict):
        raise HTTPException(status_code=400, detail="policy must be an object")
    saved = alert_service.set_policy(db, tenant_id=org_id, policy=policy)
    audit_service.log(db, tenant_id=org_id, event_type="alert_policy_updated", payload={"policy": saved})
    return {"org_id": org_id, "policy": saved}


@router.get('/api/v1/alerts/routing')
def get_alert_routing(
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    require_member_or_admin(role)
    routing = notification_service.get_routing(db, tenant_id=org_id)
    audit_service.log(db, tenant_id=org_id, event_type="alert_routing_viewed", payload={"has_channels": bool(routing.get("channels"))})
    return {"org_id": org_id, "routing": routing}


@router.post('/api/v1/alerts/routing')
def set_alert_routing(
    req: dict,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    org_id = str(req.get("org_id", ""))
    ensure_tenant_scope(tenant_id, org_id)
    require_admin(role)
    routing = req.get("routing", {})
    if not isinstance(routing, dict):
        raise HTTPException(status_code=400, detail="routing must be an object")
    saved = notification_service.set_routing(db, tenant_id=org_id, routing=routing)
    audit_service.log(db, tenant_id=org_id, event_type="alert_routing_updated", payload={"channels": len(saved.get("channels", []))})
    return {"org_id": org_id, "routing": saved}


@router.get('/api/v1/alerts/notifications')
def list_alert_notifications(
    org_id: str,
    alert_id: str | None = None,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    require_member_or_admin(role)
    rows = notification_service.list_notifications(db, tenant_id=org_id, alert_id=alert_id)
    items = [
        {
            "notification_id": n.id,
            "alert_id": n.alert_id,
            "event_type": n.event_type,
            "channel_type": n.channel_type,
            "channel_target": n.channel_target,
            "recipient": n.recipient,
            "status": n.status.value,
            "retry_count": n.retry_count,
            "next_retry_at": n.next_retry_at.isoformat() if n.next_retry_at else None,
            "last_error": n.last_error,
            "created_at": n.created_at.isoformat(),
        }
        for n in rows
    ]
    audit_service.log(db, tenant_id=org_id, event_type="alert_notifications_listed", payload={"count": len(items)})
    return {"org_id": org_id, "items": items}


@router.post('/api/v1/alerts/reminders/process')
def process_alert_reminders(
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    require_member_or_admin(role)
    rows = notification_service.queue_ack_reminders(db, tenant_id=org_id)
    audit_service.log(db, tenant_id=org_id, event_type="alert_reminders_processed", payload={"count": len(rows)})
    return {"org_id": org_id, "reminders": len(rows), "notification_ids": [r.id for r in rows]}


@router.post('/api/v1/alerts/notifications/retry')
def retry_alert_notifications(
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    require_member_or_admin(role)
    rows = notification_service.retry_failed_notifications(db, tenant_id=org_id)
    audit_service.log(db, tenant_id=org_id, event_type="alert_notifications_retried", payload={"count": len(rows)})
    return {"org_id": org_id, "retried": len(rows), "notification_ids": [r.id for r in rows]}


@router.get('/api/v1/alerts/notifications/metrics')
def alert_notification_metrics(
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    require_member_or_admin(role)
    payload = notification_service.metrics(db, tenant_id=org_id)
    audit_service.log(db, tenant_id=org_id, event_type="alert_notifications_metrics_viewed", payload=payload)
    return payload


@router.get('/api/v1/tenants/purge/preview')
def tenant_purge_preview(
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    require_admin(role)
    preview = tenant_admin_service.preview(db, tenant_id=org_id)
    payload = {
        "org_id": org_id,
        "tenant_exists": preview.tenant_exists,
        "active_workflows": preview.active_workflows,
        "queued_workflows": preview.queued_workflows,
        "estimated_cache_entries": preview.estimated_cache_entries,
        "audit_rows_retained": preview.audit_rows_retained,
        "counts": preview.counts,
    }
    audit_service.log(db, tenant_id=org_id, event_type="tenant_purge_previewed", payload=payload)
    return payload


@router.post('/api/v1/tenants/purge')
def tenant_purge_execute(
    req: dict,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    org_id = str(req.get("org_id", ""))
    force = bool(req.get("force", False))
    confirm = bool(req.get("confirm", False))
    ensure_tenant_scope(tenant_id, org_id)
    require_admin(role)
    if not org_id:
        raise HTTPException(status_code=400, detail="org_id is required")
    if not confirm:
        raise HTTPException(status_code=400, detail="confirm=true is required for purge execution")

    response = tenant_admin_service.purge(db, tenant_id=org_id, force=force)
    if response.get("status") == "blocked_active_workflows":
        raise HTTPException(status_code=409, detail=response)
    if response.get("status") == "not_found":
        raise HTTPException(status_code=404, detail="tenant not found")
    return response
