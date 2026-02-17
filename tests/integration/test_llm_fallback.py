from fastapi.testclient import TestClient

from data_autopilot.main import app


client = TestClient(app)


def _headers(org_id: str = "org_fallback") -> dict[str, str]:
    return {"X-Tenant-Id": org_id, "X-User-Role": "member"}


def test_llm_status_returns_fallback_when_no_key() -> None:
    """Without LLM_API_KEY configured, status should report fallback mode."""
    r = client.get("/api/v1/llm/status")
    assert r.status_code == 200
    body = r.json()
    assert body["mode"] == "fallback"
    assert body["configured"] is False


def test_chat_works_in_fallback_mode() -> None:
    """Chat endpoint should return a structured response even without LLM."""
    r = client.post(
        "/api/v1/chat/run",
        headers=_headers(),
        json={
            "org_id": "org_fallback",
            "user_id": "u_fallback",
            "session_id": "s_fallback",
            "message": "show me revenue for last 7 days",
        },
    )
    assert r.status_code == 200
    body = r.json()
    assert "response_type" in body
    assert "summary" in body
    assert body["meta"]["intent_action"] in {"query", "dashboard", "profile", "memo"}


def test_agent_run_works_in_fallback_mode() -> None:
    """Agent endpoint should return a structured response even without LLM."""
    r = client.post(
        "/api/v1/agent/run",
        headers=_headers(),
        json={
            "org_id": "org_fallback",
            "user_id": "u_fallback",
            "session_id": "s_fallback",
            "message": "show me DAU for the last 14 days",
        },
    )
    assert r.status_code == 200
    body = r.json()
    assert "response_type" in body
    assert "summary" in body
