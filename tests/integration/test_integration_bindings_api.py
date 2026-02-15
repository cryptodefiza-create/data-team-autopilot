from fastapi.testclient import TestClient

from data_autopilot.main import app


client = TestClient(app)


def test_integration_binding_crud_and_rbac() -> None:
    org = "org_bindings_api"
    admin = {"X-Tenant-Id": org, "X-User-Role": "admin"}
    member = {"X-Tenant-Id": org, "X-User-Role": "member"}

    forbidden = client.post(
        "/api/v1/integrations/bindings",
        json={"org_id": org, "binding_type": "slack_team", "external_id": "T-1"},
        headers=member,
    )
    assert forbidden.status_code == 403

    created = client.post(
        "/api/v1/integrations/bindings",
        json={"org_id": org, "binding_type": "slack_team", "external_id": "T-1"},
        headers=admin,
    )
    assert created.status_code == 200
    binding_id = created.json()["id"]

    listed = client.get("/api/v1/integrations/bindings", params={"org_id": org}, headers=admin)
    assert listed.status_code == 200
    assert any(item["id"] == binding_id for item in listed.json()["items"])

    deleted = client.delete(f"/api/v1/integrations/bindings/{binding_id}", params={"org_id": org}, headers=admin)
    assert deleted.status_code == 200
    assert deleted.json()["deleted"] is True
