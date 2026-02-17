from fastapi.testclient import TestClient

from data_autopilot.main import app


client = TestClient(app)


def test_artifact_lineage_and_diff_endpoints() -> None:
    org = "org_lineage_diff"
    headers = {"X-Tenant-Id": org, "X-User-Role": "member"}

    p = client.post("/api/v1/workflows/profile", params={"org_id": org}, headers=headers)
    assert p.status_code == 200
    assert p.json().get("status") == "success"

    d1 = client.post("/api/v1/workflows/dashboard", params={"org_id": org}, headers=headers)
    assert d1.status_code == 200
    b1 = d1.json()
    artifact_id = b1["artifact_id"]

    d2 = client.post("/api/v1/workflows/dashboard", params={"org_id": org}, headers=headers)
    assert d2.status_code == 200

    lineage = client.get(f"/api/v1/artifacts/{artifact_id}/lineage", params={"org_id": org}, headers=headers)
    assert lineage.status_code == 200
    lin = lineage.json()
    assert len(lin["nodes"]) >= 2
    assert len(lin["edges"]) >= 1

    diff = client.get(f"/api/v1/artifacts/{artifact_id}/diff", params={"org_id": org}, headers=headers)
    assert diff.status_code == 200
    body = diff.json()
    assert body["from_version"] < body["to_version"]
    assert isinstance(body["changes"], list)
