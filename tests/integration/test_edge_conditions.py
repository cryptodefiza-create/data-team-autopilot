from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

from fastapi.testclient import TestClient

from data_autopilot.api import routes
from data_autopilot.db.session import SessionLocal
from data_autopilot.main import app
from data_autopilot.models.entities import WorkflowQueue, WorkflowRun


client = TestClient(app)


def test_query_endpoints_invalid_inputs_and_not_found() -> None:
    org = f"org_query_invalid_{uuid4().hex[:8]}"
    headers = {"X-Tenant-Id": org, "X-User-Role": "member"}

    empty_sql = client.post("/api/v1/queries/preview", headers=headers, json={"org_id": org, "sql": ""})
    assert empty_sql.status_code == 400
    assert "sql is required" in empty_sql.json()["detail"]

    missing_preview_id = client.post("/api/v1/queries/approve-run", headers=headers, json={"org_id": org})
    assert missing_preview_id.status_code == 400
    assert "preview_id is required" in missing_preview_id.json()["detail"]

    not_found = client.post(
        "/api/v1/queries/approve-run",
        headers=headers,
        json={"org_id": org, "preview_id": "qry_missing"},
    )
    assert not_found.status_code == 404
    assert "preview_id not found" in not_found.json()["detail"]


def test_workflow_retry_invalid_type_and_cancel_missing() -> None:
    org = f"org_wf_invalid_{uuid4().hex[:8]}"
    headers = {"X-Tenant-Id": org, "X-User-Role": "member"}

    invalid_retry = client.post(
        "/api/v1/workflows/retry",
        params={"org_id": org, "workflow_type": "unknown", "action": "retry"},
        headers=headers,
    )
    assert invalid_retry.status_code == 400
    assert "Unsupported workflow_type" in invalid_retry.json()["detail"]

    missing_cancel = client.post(
        "/api/v1/workflows/wf_does_not_exist/cancel",
        params={"org_id": org},
        headers=headers,
    )
    assert missing_cancel.status_code == 404
    assert "workflow not found" in missing_cancel.json()["detail"]


def test_tenant_purge_requires_admin_and_confirm() -> None:
    org = f"org_purge_guard_{uuid4().hex[:8]}"
    member = {"X-Tenant-Id": org, "X-User-Role": "member"}
    admin = {"X-Tenant-Id": org, "X-User-Role": "admin"}

    preview_as_member = client.get("/api/v1/tenants/purge/preview", params={"org_id": org}, headers=member)
    assert preview_as_member.status_code == 403

    execute_no_confirm = client.post("/api/v1/tenants/purge", headers=admin, json={"org_id": org, "force": True})
    assert execute_no_confirm.status_code == 400
    assert "confirm=true is required" in execute_no_confirm.json()["detail"]


def test_process_queue_deferred_due_capacity_boundary() -> None:
    org = f"org_queue_capacity_{uuid4().hex[:8]}"
    headers = {"X-Tenant-Id": org, "X-User-Role": "member"}
    max_wf = routes.workflow_service.settings.per_org_max_workflows

    db = SessionLocal()
    try:
        for i in range(max_wf):
            db.add(WorkflowRun(id=f"wf_run_{org}_{i}", tenant_id=org, workflow_type="profile", status="running"))
        for i in range(2):
            db.add(
                WorkflowQueue(
                    id=f"wf_q_{org}_{i}",
                    tenant_id=org,
                    workflow_type="profile",
                    payload={"org_id": org},
                    status="queued",
                    reason="capacity_test",
                )
            )
        db.commit()
    finally:
        db.close()

    processed = client.post("/api/v1/workflows/process-queue", params={"org_id": org}, headers=headers)
    assert processed.status_code == 200
    body = processed.json()
    assert body["processed"] == 0
    assert body["deferred_due_capacity"] >= 2
    assert body["queued_total"] >= 2

    db = SessionLocal()
    try:
        queued_rows = db.query(WorkflowQueue).filter(WorkflowQueue.tenant_id == org, WorkflowQueue.status == "queued").count()
        assert queued_rows >= 2
    finally:
        db.close()


def test_memo_output_matches_packet_values_and_hash_shape() -> None:
    org = f"org_memo_output_{uuid4().hex[:8]}"
    headers = {"X-Tenant-Id": org, "X-User-Role": "member"}

    profile = client.post("/api/v1/workflows/profile", params={"org_id": org}, headers=headers)
    assert profile.status_code == 200
    assert profile.json()["status"] == "success"

    memo = client.post("/api/v1/workflows/memo", params={"org_id": org}, headers=headers)
    assert memo.status_code == 200
    payload = memo.json()
    assert payload["status"] == "success"
    assert payload["validation"]["passed"] is True

    artifact_id = payload["artifact_id"]
    art = client.get(f"/api/v1/artifacts/{artifact_id}", params={"org_id": org}, headers=headers)
    assert art.status_code == 200
    data = art.json()["data"]
    packet = data["packet"]
    memo_data = data["memo"]
    packet_hash = data["packet_hash"]

    assert isinstance(packet_hash, str)
    assert len(packet_hash) == 64
    assert all(ch in "0123456789abcdef" for ch in packet_hash.lower())

    packet_by_metric = {k["metric_name"]: k for k in packet["kpis"]}
    for change in memo_data["key_changes"]:
        metric = change["metric_name"]
        assert metric in packet_by_metric
        kpi = packet_by_metric[metric]
        assert change["current"] == kpi["current_value"]
        assert change["previous"] == kpi["previous_value"]
        assert change["delta_pct"] == kpi["delta_percent"]


def test_concurrent_profile_requests_when_capacity_saturated_queue_all() -> None:
    org = f"org_concurrent_queue_{uuid4().hex[:8]}"
    headers = {"X-Tenant-Id": org, "X-User-Role": "member"}
    max_wf = routes.workflow_service.settings.per_org_max_workflows

    db = SessionLocal()
    try:
        for i in range(max_wf):
            db.add(WorkflowRun(id=f"wf_busy_{org}_{i}", tenant_id=org, workflow_type="profile", status="running"))
        db.commit()
    finally:
        db.close()

    def call_profile() -> int:
        resp = client.post("/api/v1/workflows/profile", params={"org_id": org}, headers=headers)
        return resp.status_code, resp.json().get("workflow_status")

    with ThreadPoolExecutor(max_workers=5) as pool:
        results = list(pool.map(lambda _: call_profile(), range(5)))

    assert all(status == 200 for status, _ in results)
    assert all(workflow_status == "queued" for _, workflow_status in results)
