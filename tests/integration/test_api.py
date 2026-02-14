from fastapi.testclient import TestClient

from data_autopilot.main import app


client = TestClient(app)


def test_health() -> None:
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_agent_run() -> None:
    org = "org_1"
    payload = {
        "org_id": org,
        "user_id": "user_1",
        "message": "show me dau",
        "session_id": "sess_1",
    }
    r = client.post(
        "/api/v1/agent/run",
        json=payload,
        headers={"X-Tenant-Id": org, "X-User-Role": "member"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["response_type"] in {"query_result", "blocked"}
