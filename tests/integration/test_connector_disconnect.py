from fastapi.testclient import TestClient
from uuid import uuid4

from data_autopilot.db.session import SessionLocal
from data_autopilot.main import app
from data_autopilot.models.entities import WorkflowQueue, WorkflowRun


client = TestClient(app)


def test_disconnect_cancels_active_and_queued_workflows() -> None:
    org = "org_disconnect_flow"
    admin_headers = {"X-Tenant-Id": org, "X-User-Role": "admin"}

    conn = client.post(
        "/api/v1/connectors/bigquery",
        headers=admin_headers,
        json={"org_id": org, "service_account_json": {"client_email": "disc@example.com"}},
    )
    assert conn.status_code == 200
    conn_id = conn.json()["connection_id"]

    db = SessionLocal()
    run_id = f"wf_disc_{uuid4().hex[:8]}"
    queue_id = f"q_disc_{uuid4().hex[:8]}"
    try:
        db.add(WorkflowRun(id=run_id, tenant_id=org, workflow_type="profile", status="running"))
        db.add(WorkflowQueue(id=queue_id, tenant_id=org, workflow_type="memo", payload={}, status="queued", reason="test"))
        db.commit()
    finally:
        db.close()

    disc = client.post(
        f"/api/v1/connectors/{conn_id}/disconnect",
        params={"org_id": org},
        headers=admin_headers,
    )
    assert disc.status_code == 200
    body = disc.json()
    assert body["status"] == "disconnected"
    assert body["cancelled_active_workflows"] >= 1
    assert body["cancelled_queued_workflows"] >= 1

    db = SessionLocal()
    try:
        run = db.query(WorkflowRun).filter(WorkflowRun.id == run_id).one()
        queue = db.query(WorkflowQueue).filter(WorkflowQueue.id == queue_id).one()
        assert run.status == "cancelled"
        assert queue.status == "cancelled"
    finally:
        db.close()
