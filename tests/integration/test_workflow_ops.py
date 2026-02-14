from uuid import uuid4

from fastapi.testclient import TestClient

from data_autopilot.db.session import SessionLocal
from data_autopilot.main import app
from data_autopilot.models.entities import WorkflowRun


client = TestClient(app)


def test_list_and_cancel_workflow_run() -> None:
    org = "org_workflow_ops"
    headers = {"X-Tenant-Id": org, "X-User-Role": "member"}
    run_id = f"wf_ops_{uuid4().hex[:8]}"

    db = SessionLocal()
    try:
        db.add(WorkflowRun(id=run_id, tenant_id=org, workflow_type="memo", status="running"))
        db.commit()
    finally:
        db.close()

    listed = client.get("/api/v1/workflows/runs", params={"org_id": org, "status": "running"}, headers=headers)
    assert listed.status_code == 200
    assert any(i["workflow_id"] == run_id for i in listed.json()["items"])

    cancelled = client.post(f"/api/v1/workflows/{run_id}/cancel", params={"org_id": org}, headers=headers)
    assert cancelled.status_code == 200
    assert cancelled.json()["status"] == "cancelled"

    listed2 = client.get("/api/v1/workflows/runs", params={"org_id": org}, headers=headers)
    assert listed2.status_code == 200
    rows = [i for i in listed2.json()["items"] if i["workflow_id"] == run_id]
    assert rows
    assert rows[0]["status"] == "cancelled"
