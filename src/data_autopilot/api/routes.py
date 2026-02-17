from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from data_autopilot.api.core_routes import router as core_router
from data_autopilot.api.integration_routes import router as integration_router
from data_autopilot.api.workflow_routes import router as workflow_router
from data_autopilot.api.state import (
    alert_service,
    artifact_service,
    audit_service,
    connector_service,
    notification_service,
    query_service,
    tenant_admin_service,
    integration_binding_service,
)
from data_autopilot.db.session import get_db
from data_autopilot.models.entities import Role
from data_autopilot.security.rbac import require_admin, require_member_or_admin, role_from_headers
from data_autopilot.security.tenancy import ensure_tenant_scope, tenant_from_headers
from data_autopilot.schemas.common import (
    ConnectorRequest,
    ConnectorResponse,
)
from data_autopilot.models.entities import AlertSeverity, AlertStatus
from data_autopilot.models.entities import CatalogColumn
from data_autopilot.models.entities import IntegrationBindingType

router = APIRouter()
router.include_router(core_router)
router.include_router(workflow_router)
router.include_router(integration_router)


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
        result = db.execute(
            update(CatalogColumn)
            .where(
                CatalogColumn.tenant_id == org_id,
                CatalogColumn.dataset == dataset,
                CatalogColumn.table_name == table,
                CatalogColumn.column_name == column,
            )
            .values(is_pii=is_pii)
        )
        updated += int(result.rowcount or 0)
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


@router.get('/api/v1/integrations/bindings')
def list_integration_bindings(
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    require_admin(role)
    rows = integration_binding_service.list_for_tenant(db, tenant_id=org_id)
    items = [
        {
            "id": row.id,
            "binding_type": row.binding_type.value,
            "external_id": row.external_id,
            "created_at": row.created_at.isoformat(),
        }
        for row in rows
    ]
    audit_service.log(db, tenant_id=org_id, event_type="integration_bindings_listed", payload={"count": len(items)})
    return {"org_id": org_id, "items": items}


@router.post('/api/v1/integrations/bindings')
def upsert_integration_binding(
    req: dict,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    org_id = str(req.get("org_id", ""))
    binding_type_raw = str(req.get("binding_type", ""))
    external_id = str(req.get("external_id", "")).strip()
    ensure_tenant_scope(tenant_id, org_id)
    require_admin(role)
    if not external_id:
        raise HTTPException(status_code=400, detail="external_id is required")
    try:
        binding_type = IntegrationBindingType(binding_type_raw)
    except ValueError:
        allowed = [v.value for v in IntegrationBindingType]
        raise HTTPException(status_code=400, detail=f"invalid binding_type; allowed: {allowed}")
    row = integration_binding_service.upsert(
        db,
        tenant_id=org_id,
        binding_type=binding_type,
        external_id=external_id,
    )
    audit_service.log(
        db,
        tenant_id=org_id,
        event_type="integration_binding_upserted",
        payload={"binding_id": row.id, "binding_type": row.binding_type.value},
    )
    return {
        "org_id": org_id,
        "id": row.id,
        "binding_type": row.binding_type.value,
        "external_id": row.external_id,
    }


@router.delete('/api/v1/integrations/bindings/{binding_id}')
def delete_integration_binding(
    binding_id: int,
    org_id: str,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    ensure_tenant_scope(tenant_id, org_id)
    require_admin(role)
    deleted = integration_binding_service.delete(db, tenant_id=org_id, binding_id=binding_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="binding not found")
    audit_service.log(
        db,
        tenant_id=org_id,
        event_type="integration_binding_deleted",
        payload={"binding_id": binding_id},
    )
    return {"org_id": org_id, "deleted": True}


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


@router.post('/api/v1/admin/setup-tester-org')
def setup_tester_org(
    req: dict,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(tenant_from_headers),
    role: Role = Depends(role_from_headers),
) -> dict:
    """One-shot idempotent setup: create tenant, connect mock BQ, run profiler."""
    from data_autopilot.models.entities import Tenant, Connection, CatalogTable, CatalogColumn
    from data_autopilot.api.state import workflow_service

    org_id = str(req.get("org_id", tenant_id))
    ensure_tenant_scope(tenant_id, org_id)
    require_admin(role)

    tenant_exists = db.query(Tenant).filter(Tenant.id == org_id).first() is not None
    if not tenant_exists:
        db.add(Tenant(id=org_id, name=org_id, settings={}))
        db.commit()

    conn = db.query(Connection).filter(Connection.tenant_id == org_id, Connection.status == "active").first()
    if conn is None:
        conn = Connection(id=f"conn_{org_id}", tenant_id=org_id, status="active", config_encrypted={})
        db.add(conn)
        db.commit()

    workflow_service.run_profile_flow(db, tenant_id=org_id)

    tables = db.query(CatalogTable).filter(CatalogTable.tenant_id == org_id).all()
    pii_cols = (
        db.query(CatalogColumn)
        .filter(CatalogColumn.tenant_id == org_id, CatalogColumn.pii_confidence >= 80)
        .all()
    )

    summary = {
        "org_id": org_id,
        "tenant_exists": tenant_exists,
        "tables_discovered": len(tables),
        "table_names": [t.table_name for t in tables],
        "pii_columns_flagged": len(pii_cols),
        "pii_details": [
            {"table": c.table_name, "column": c.column_name, "confidence": c.pii_confidence}
            for c in pii_cols
        ],
    }

    audit_service.log(
        db,
        tenant_id=org_id,
        event_type="tester_org_setup",
        payload=summary,
    )

    return summary
