from fastapi.testclient import TestClient

from data_autopilot.main import app


client = TestClient(app)


def test_pii_review_and_confirm_flow() -> None:
    org = "org_pii_review"
    headers = {"X-Tenant-Id": org, "X-User-Role": "member"}

    prof = client.post("/api/v1/workflows/profile", params={"org_id": org}, headers=headers)
    assert prof.status_code == 200
    assert prof.json().get("status") == "success"

    review = client.get("/api/v1/pii/review", params={"org_id": org}, headers=headers)
    assert review.status_code == 200
    body = review.json()
    assert len(body["auto_tagged_high_confidence"]) >= 1

    target = body["auto_tagged_high_confidence"][0]
    confirm = client.post(
        "/api/v1/pii/review/confirm",
        params={"org_id": org},
        headers=headers,
        json=[
            {
                "dataset": target["dataset"],
                "table": target["table"],
                "column": target["column"],
                "is_pii": False,
            }
        ],
    )
    assert confirm.status_code == 200
    assert confirm.json()["updated"] >= 1

    review2 = client.get("/api/v1/pii/review", params={"org_id": org}, headers=headers)
    assert review2.status_code == 200
    columns = review2.json()["auto_tagged_high_confidence"]
    changed = [c for c in columns if c["column"] == target["column"]]
    if changed:
        assert changed[0]["is_pii"] is False
