from fastapi.testclient import TestClient

from data_autopilot.main import app


client = TestClient(app)


def test_profile_workflow_endpoint() -> None:
    org = "org_1"
    r = client.post(
        "/api/v1/workflows/profile",
        params={"org_id": org},
        headers={"X-Tenant-Id": org, "X-User-Role": "member"},
    )
    assert r.status_code == 200
    assert r.json()["status"] == "success"


def test_dashboard_workflow_endpoint() -> None:
    org = "org_1"
    r = client.post(
        "/api/v1/workflows/dashboard",
        params={"org_id": org},
        headers={"X-Tenant-Id": org, "X-User-Role": "member"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "success"
    assert body["artifact_id"].startswith("art_")


def test_memo_workflow_endpoint() -> None:
    org = "org_1"
    r = client.post(
        "/api/v1/workflows/memo",
        params={"org_id": org},
        headers={"X-Tenant-Id": org, "X-User-Role": "member"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "success"
    assert body["artifact_id"].startswith("art_")
