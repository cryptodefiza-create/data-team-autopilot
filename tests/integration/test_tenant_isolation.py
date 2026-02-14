from fastapi.testclient import TestClient

from data_autopilot.main import app


client = TestClient(app)


def test_cross_tenant_request_blocked() -> None:
    org_in_body = "org_target"
    attacker_header = {"X-Tenant-Id": "org_other", "X-User-Role": "member"}

    payload = {
        "org_id": org_in_body,
        "user_id": "u1",
        "message": "show me dau",
        "session_id": "s1",
    }
    r = client.post("/api/v1/agent/run", json=payload, headers=attacker_header)
    assert r.status_code == 403


def test_missing_tenant_header_blocked() -> None:
    r = client.post("/api/v1/workflows/profile", params={"org_id": "org_no_header"})
    assert r.status_code == 400
