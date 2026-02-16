from fastapi.testclient import TestClient

from data_autopilot.main import app


client = TestClient(app)


def _headers(org_id: str = "org_chat") -> dict[str, str]:
    return {"X-Tenant-Id": org_id, "X-User-Role": "member"}


def test_chat_ui_page_renders() -> None:
    response = client.get("/chat")
    assert response.status_code == 200
    assert "/api/v1/chat/run" in response.text


def test_chat_run_query_path() -> None:
    response = client.post(
        "/api/v1/chat/run",
        headers=_headers(),
        json={"org_id": "org_chat", "user_id": "u1", "message": "show me dau", "session_id": "s1"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["response_type"] in {"query_result", "approval_required", "blocked"}
    assert body["meta"]["intent_action"] in {"query", "dashboard", "profile", "memo"}


def test_chat_run_dashboard_path() -> None:
    response = client.post(
        "/api/v1/chat/run",
        headers=_headers(),
        json={"org_id": "org_chat", "user_id": "u1", "message": "create an executive dashboard", "session_id": "s2"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["response_type"] in {"workflow_result", "queued"}
    assert body["meta"]["intent_action"] == "dashboard"
