from fastapi.testclient import TestClient

from data_autopilot.main import app


client = TestClient(app)


def test_dashboard_regeneration_increments_version() -> None:
    org_id = "org_versioning"
    headers = {"X-Tenant-Id": org_id, "X-User-Role": "member"}
    r1 = client.post("/api/v1/workflows/profile", params={"org_id": org_id}, headers=headers)
    assert r1.status_code == 200

    d1 = client.post("/api/v1/workflows/dashboard", params={"org_id": org_id}, headers=headers)
    d2 = client.post("/api/v1/workflows/dashboard", params={"org_id": org_id}, headers=headers)

    assert d1.status_code == 200
    assert d2.status_code == 200
    assert d2.json()["version"] >= d1.json()["version"]
