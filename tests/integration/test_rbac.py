from fastapi.testclient import TestClient

from data_autopilot.main import app


client = TestClient(app)


def test_viewer_blocked_from_agent_and_workflows() -> None:
    org = "org_rbac_viewer"
    headers = {"X-Tenant-Id": org, "X-User-Role": "viewer"}

    agent = client.post(
        "/api/v1/agent/run",
        json={"org_id": org, "user_id": "u1", "message": "show me dau", "session_id": "s1"},
        headers=headers,
    )
    assert agent.status_code == 403

    profile = client.post("/api/v1/workflows/profile", params={"org_id": org}, headers=headers)
    assert profile.status_code == 403


def test_admin_required_for_connector_ops() -> None:
    org = "org_rbac_admin"
    member_headers = {"X-Tenant-Id": org, "X-User-Role": "member"}
    admin_headers = {"X-Tenant-Id": org, "X-User-Role": "admin"}

    member_connect = client.post(
        "/api/v1/connectors/bigquery",
        json={"org_id": org, "service_account_json": {"client_email": "a@b.com"}},
        headers=member_headers,
    )
    assert member_connect.status_code == 403

    admin_connect = client.post(
        "/api/v1/connectors/bigquery",
        json={"org_id": org, "service_account_json": {"client_email": "a@b.com"}},
        headers=admin_headers,
    )
    assert admin_connect.status_code == 200

    conn_id = admin_connect.json()["connection_id"]

    member_disconnect = client.post(
        f"/api/v1/connectors/{conn_id}/disconnect",
        params={"org_id": org},
        headers=member_headers,
    )
    assert member_disconnect.status_code == 403

    admin_disconnect = client.post(
        f"/api/v1/connectors/{conn_id}/disconnect",
        params={"org_id": org},
        headers=admin_headers,
    )
    assert admin_disconnect.status_code == 200
