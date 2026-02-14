from fastapi.testclient import TestClient
from uuid import uuid4

from data_autopilot.api import routes
from data_autopilot.db.session import SessionLocal
from data_autopilot.main import app
from data_autopilot.models.entities import WorkflowDeadLetter, WorkflowQueue, WorkflowRun


client = TestClient(app)


def test_concurrency_limit_queues_request() -> None:
    org = "org_concurrency_limit"
    db = SessionLocal()
    try:
        for i in range(3):
            db.add(
                WorkflowRun(
                    id=f"wf_running_{uuid4().hex[:8]}_{i}",
                    tenant_id=org,
                    workflow_type="memo",
                    status="running",
                )
            )
        db.commit()
    finally:
        db.close()

    r = client.post(
        "/api/v1/workflows/memo",
        params={"org_id": org},
        headers={"X-Tenant-Id": org, "X-User-Role": "member"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["workflow_status"] == "queued"
    assert body["reason"] == "concurrency_limit"
    assert body["queue_position"] >= 1


def test_profile_flow_resume_from_partial_failure() -> None:
    org = "org_resume_profile"
    db = SessionLocal()
    try:
        first = routes.workflow_service.run_profile_flow(
            db,
            tenant_id=org,
            payload={"failure_modes": {"profile_columns": {"mode": "permission_denied", "remaining": 2}}},
        )
        assert first["workflow_status"] == "partial_failure"
        workflow_id = first["workflow_id"]

        second = routes.workflow_service.run_profile_flow(db, tenant_id=org, workflow_id=workflow_id)
        assert second["status"] == "success"
        assert second["workflow_id"] == workflow_id
        assert second["resumed"] is True
    finally:
        db.close()


def test_dead_letter_after_three_failed_queue_attempts() -> None:
    org = "org_dlq"
    headers = {"X-Tenant-Id": org, "X-User-Role": "member"}

    db = SessionLocal()
    try:
        queued = routes.degradation_service.enqueue(
            db,
            tenant_id=org,
            workflow_type="memo",
            payload={"org_id": org, "failure_modes": {"store_artifact": {"mode": "permission_denied", "remaining": 10}}},
            reason="llm_unavailable",
        )
        assert queued["workflow_status"] == "queued"
    finally:
        db.close()

    for _ in range(3):
        processed = client.post("/api/v1/workflows/process-queue", params={"org_id": org}, headers=headers)
        assert processed.status_code == 200

    db = SessionLocal()
    try:
        q = db.query(WorkflowQueue).filter(WorkflowQueue.tenant_id == org).first()
        assert q is not None
        assert q.status == "dead_letter"
        dlq = db.query(WorkflowDeadLetter).filter(WorkflowDeadLetter.tenant_id == org).first()
        assert dlq is not None
        assert dlq.workflow_type == "memo"
        assert len(dlq.error_history) >= 3
    finally:
        db.close()

    dlq_resp = client.get("/api/v1/workflows/dead-letters", params={"org_id": org}, headers=headers)
    assert dlq_resp.status_code == 200
    assert len(dlq_resp.json()["items"]) >= 1


def test_queue_status_endpoint() -> None:
    org = "org_queue_status"
    headers = {"X-Tenant-Id": org, "X-User-Role": "member"}
    old = routes.degradation_service.settings.simulate_llm_unavailable
    routes.degradation_service.settings.simulate_llm_unavailable = True
    try:
        queued = client.post("/api/v1/workflows/memo", params={"org_id": org}, headers=headers)
        assert queued.status_code == 200
        assert queued.json()["workflow_status"] == "queued"
    finally:
        routes.degradation_service.settings.simulate_llm_unavailable = old

    status = client.get("/api/v1/workflows/queue", params={"org_id": org}, headers=headers)
    assert status.status_code == 200
    body = status.json()
    assert body["queued_total"] >= 1
    assert body["items"][0]["position"] >= 1


def test_retry_with_sampling_action_applies_sampling_mode() -> None:
    org = "org_retry_sampling"
    headers = {"X-Tenant-Id": org, "X-User-Role": "member"}

    db = SessionLocal()
    try:
        first = routes.workflow_service.run_profile_flow(
            db,
            tenant_id=org,
            payload={"failure_modes": {"profile_columns": {"mode": "timeout", "remaining": 10}}},
        )
        assert first["workflow_status"] == "partial_failure"
        workflow_id = first["workflow_id"]
    finally:
        db.close()

    retried = client.post(
        "/api/v1/workflows/retry",
        params={
            "org_id": org,
            "workflow_type": "profile",
            "workflow_id": workflow_id,
            "action": "retry_with_sampling",
        },
        headers=headers,
    )
    assert retried.status_code == 200
    body = retried.json()
    assert body["status"] == "success"
    assert body["sampling_mode"] is True


def test_skip_and_continue_action_marks_success() -> None:
    org = "org_retry_skip"
    headers = {"X-Tenant-Id": org, "X-User-Role": "member"}

    db = SessionLocal()
    try:
        first = routes.workflow_service.run_profile_flow(
            db,
            tenant_id=org,
            payload={"failure_modes": {"profile_columns": {"mode": "permission_denied", "remaining": 2}}},
        )
        assert first["workflow_status"] == "partial_failure"
        workflow_id = first["workflow_id"]
    finally:
        db.close()

    retried = client.post(
        "/api/v1/workflows/retry",
        params={
            "org_id": org,
            "workflow_type": "profile",
            "workflow_id": workflow_id,
            "action": "skip_and_continue",
        },
        headers=headers,
    )
    assert retried.status_code == 200
    body = retried.json()
    assert body["status"] == "success"
    assert body["skip_on_error"] is True
