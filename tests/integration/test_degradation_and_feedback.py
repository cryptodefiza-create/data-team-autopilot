from fastapi.testclient import TestClient

from data_autopilot.api.state import degradation_service
from data_autopilot.main import app


client = TestClient(app)


def test_feedback_summary_endpoint() -> None:
    org = "org_fb_summary"
    headers = {"X-Tenant-Id": org, "X-User-Role": "member"}
    # create minimal artifact-producing flow
    memo = client.post("/api/v1/workflows/memo", params={"org_id": org}, headers=headers).json()
    fb = {
        "tenant_id": org,
        "user_id": "u1",
        "artifact_id": memo["artifact_id"],
        "artifact_version": memo.get("version", 1),
        "artifact_type": "memo",
        "feedback_type": "negative",
        "comment": "check",
        "prompt_hash": "ph_1",
    }
    r = client.post("/api/v1/feedback", json=fb, headers=headers)
    assert r.status_code == 200

    summary = client.get("/api/v1/feedback/summary", params={"org_id": org}, headers=headers)
    assert summary.status_code == 200
    body = summary.json()
    assert "artifact_feedback" in body
    assert "memo" in body["artifact_feedback"]


def test_workflow_queue_when_warehouse_unavailable() -> None:
    old = degradation_service.settings.simulate_warehouse_unavailable
    degradation_service.settings.simulate_warehouse_unavailable = True
    try:
        org = "org_queue"
        r = client.post(
            "/api/v1/workflows/profile",
            params={"org_id": org},
            headers={"X-Tenant-Id": org, "X-User-Role": "member"},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["workflow_status"] == "queued"
        assert body["reason"] == "warehouse_unavailable"
    finally:
        degradation_service.settings.simulate_warehouse_unavailable = old


def test_process_queue() -> None:
    org = "org_queue_process"
    headers = {"X-Tenant-Id": org, "X-User-Role": "member"}
    old = degradation_service.settings.simulate_llm_unavailable
    degradation_service.settings.simulate_llm_unavailable = True
    try:
        queued = client.post("/api/v1/workflows/memo", params={"org_id": org}, headers=headers).json()
        assert queued["workflow_status"] == "queued"
    finally:
        degradation_service.settings.simulate_llm_unavailable = old

    processed = client.post("/api/v1/workflows/process-queue", params={"org_id": org}, headers=headers)
    assert processed.status_code == 200
    assert processed.json()["processed"] >= 1
