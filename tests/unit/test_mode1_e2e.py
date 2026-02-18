from fastapi.testclient import TestClient

from data_autopilot.main import app

client = TestClient(app)


def _headers(org_id: str = "org_mode1") -> dict[str, str]:
    return {"X-Tenant-Id": org_id, "X-User-Role": "member"}


def test_blockchain_intent_detected() -> None:
    """1.14: Blockchain question routes to blockchain action."""
    response = client.post(
        "/api/v1/chat/run",
        headers=_headers(),
        json={
            "org_id": "org_mode1",
            "user_id": "u1",
            "message": "Show me top 10 holders of $BONK",
            "session_id": "s_bc1",
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["intent_action"] == "blockchain"


def test_non_blockchain_regression() -> None:
    """1.15: Non-blockchain question does NOT route to blockchain action."""
    response = client.post(
        "/api/v1/chat/run",
        headers=_headers(),
        json={
            "org_id": "org_mode1",
            "user_id": "u1",
            "message": "show me dau for last 14 days",
            "session_id": "s_bc2",
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["intent_action"] != "blockchain"
