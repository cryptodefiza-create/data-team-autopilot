from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from datetime import datetime

from data_autopilot.config.settings import get_settings
from data_autopilot.db.session import get_db
from data_autopilot.models.entities import Role, WorkflowQueue, WorkflowRun
from data_autopilot.security.rbac import require_admin, require_member_or_admin, role_from_headers
from data_autopilot.security.tenancy import ensure_tenant_scope, tenant_from_headers
from data_autopilot.api.state import (
    alert_service,
    audit_service,
    auto_alert_from_memo_anomalies,
    auto_alert_from_workflow_result,
    degradation_service,
    workflow_service,
)
from data_autopilot.models.entities import AlertSeverity


router = APIRouter()


def _run_or_queue(db: Session, org_id: str, workflow_type: str) -> dict:
    if not workflow_service.has_capacity(db, tenant_id=org_id, workflow_type=workflow_type):
        queued = degradation_service.enqueue(
            db,
            tenant_id=org_id,
            workflow_type=workflow_type,
            payload={"org_id": org_id},
            reason="concurrency_limit",
        )
        audit_service.log(
            db,
            tenant_id=org_id,
            event_type="workflow_queued",
            payload={"workflow_type": workflow_type, "reason": "concurrency_limit", "queue_id": queued.get("queue_id")},
        )
        return queued

    if workflow_type in {"profile", "dashboard"} and not degradation_service.warehouse_available():
        queued = degradation_service.enqueue(
            db,
            tenant_id=org_id,
            workflow_type=workflow_type,
            payload={"org_id": org_id},
            reason="warehouse_unavailable",
        )
        audit_service.log(
            db, tenant_id=org_id, event_type="workflow_queued", payload={"workflow_type": workflow_type, "reason": "warehouse_unavailable"}
        )
        return queued
    if workflow_type == "memo" and not degradation_service.llm_available():
        queued = degradation_service.enqueue(
            db,
            tenant_id=org_id,
            workflow_type=workflow_type,
            payload={"org_id": org_id},
            reason="llm_unavailable",
        )
        audit_service.log(
            db, tenant_id=org_id, event_type="workflow_queued", payload={"workflow_type": workflow_type, "reason": "llm_unavailable"}
        )
        return queued

    if workflow_type == "profile":
        result = workflow_service.run_profile_flow(db, tenant_id=org_id)
    elif workflow_type == "dashboard":
        result = workflow_service.run_dashboard_flow(db, tenant_id=org_id)
    else:
        result = workflow_service.run_memo_flow(db, tenant_id=org_id)

    auto_alert_from_workflow_result(db, org_id=org_id, workflow_type=workflow_type, result=result)
    if workflow_type == "memo" and result.get("status") == "success" and result.get("artifact_id"):
        auto_alert_from_memo_anomalies(db, org_id=org_id, artifact_id=str(result["artifact_id"]))

    audit_payload = {"workflow_type": workflow_type, "status": result.get("status")}
    if result.get("artifact_id"):
        audit_payload["artifact_id"] = result.get("artifact_id")
    audit_service.log(db, tenant_id=org_id, event_type="workflow_run", payload=audit_payload)
    return result


@router.post('/api/v1/workflows/profile')
def run_profile_workflow(
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    require_member_or_admin(role)
    return _run_or_queue(db, org_id, workflow_type="profile")


@router.post('/api/v1/workflows/dashboard')
def run_dashboard_workflow(
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    require_member_or_admin(role)
    return _run_or_queue(db, org_id, workflow_type="dashboard")


@router.post('/api/v1/workflows/memo')
def run_memo_workflow(
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    require_member_or_admin(role)
    return _run_or_queue(db, org_id, workflow_type="memo")


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


@router.post('/api/v1/workflows/cancel-all')
def cancel_all_running(
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    """Cancel all running workflows and clear queued items for a tenant."""
    ensure_tenant_scope(tenant_id, org_id)
    require_admin(role)
    runs = db.execute(
        select(WorkflowRun).where(WorkflowRun.tenant_id == org_id, WorkflowRun.status == "running")
    ).scalars().all()
    for run in runs:
        run.status = "cancelled"
        run.finished_at = datetime.utcnow()
        db.add(run)

    queued = db.execute(
        select(WorkflowQueue).where(WorkflowQueue.tenant_id == org_id, WorkflowQueue.status == "queued")
    ).scalars().all()
    for q in queued:
        q.status = "cancelled"
        q.processed_at = datetime.utcnow()
        db.add(q)

    db.commit()
    result = {"cancelled_runs": len(runs), "cancelled_queued": len(queued)}
    audit_service.log(db, tenant_id=org_id, event_type="workflows_cancel_all", payload=result)
    return result
