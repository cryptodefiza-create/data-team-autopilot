from fastapi.testclient import TestClient

from data_autopilot.main import app


client = TestClient(app)


def test_artifact_versions_and_wow() -> None:
    org = "org_art_hist"
    headers = {"X-Tenant-Id": org, "X-User-Role": "member"}

    # Build profile + memo twice to create version history
    client.post("/api/v1/workflows/profile", params={"org_id": org}, headers=headers)
    m1 = client.post("/api/v1/workflows/memo", params={"org_id": org}, headers=headers).json()
    m2 = client.post("/api/v1/workflows/memo", params={"org_id": org}, headers=headers).json()

    assert m2["version"] >= m1["version"]
    artifact_id = m2["artifact_id"]

    versions = client.get(f"/api/v1/artifacts/{artifact_id}/versions", params={"org_id": org}, headers=headers)
    assert versions.status_code == 200
    items = versions.json()["items"]
    assert len(items) >= 2

    wow = client.get(f"/api/v1/memos/{artifact_id}/wow", params={"org_id": org}, headers=headers)
    assert wow.status_code == 200
    body = wow.json()
    assert "rows" in body


def test_list_and_get_artifacts() -> None:
    org = "org_art_list"
    headers = {"X-Tenant-Id": org, "X-User-Role": "member"}
    client.post("/api/v1/workflows/profile", params={"org_id": org}, headers=headers)
    dash = client.post("/api/v1/workflows/dashboard", params={"org_id": org}, headers=headers).json()

    lst = client.get("/api/v1/artifacts", params={"org_id": org}, headers=headers)
    assert lst.status_code == 200
    assert len(lst.json()["items"]) >= 1

    get_one = client.get(f"/api/v1/artifacts/{dash['artifact_id']}", params={"org_id": org}, headers=headers)
    assert get_one.status_code == 200
    assert get_one.json()["artifact_id"] == dash["artifact_id"]

    missing = client.get("/api/v1/artifacts/art_missing", params={"org_id": org}, headers=headers)
    assert missing.status_code == 404
