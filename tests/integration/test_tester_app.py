from fastapi.testclient import TestClient

from data_autopilot.main import app

client = TestClient(app)


def test_tester_app_returns_200() -> None:
    r = client.get("/tester-app")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/html")


def test_tester_app_contains_ui_elements() -> None:
    r = client.get("/tester-app")
    body = r.text
    assert "Data Team Autopilot" in body
    assert "Tester Preview" in body
    assert "Profile Warehouse" in body
    assert "Build Dashboard" in body
    assert "Generate Memo" in body
    assert "Model Comparison" in body
    assert "LLM Usage" in body


def test_tester_app_references_api_endpoints() -> None:
    r = client.get("/tester-app")
    body = r.text
    assert "/api/v1/chat/run" in body
    assert "/api/v1/llm/status" in body
    assert "/api/v1/feedback" in body
    assert "/api/v1/llm/evaluate-memo" in body
    assert "/api/v1/artifacts" in body
    assert "/api/v1/llm/usage" in body
    assert "/api/v1/llm/budget" in body
    assert "/ready" in body
    assert "/api/v1/feedback/review" in body
    assert "provider-summary" in body


def test_tester_app_has_feedback_ui() -> None:
    r = client.get("/tester-app")
    body = r.text
    assert "My Feedback" in body
    assert "feedbackCount" in body
    assert "showToast" in body


def test_tester_app_has_dark_theme() -> None:
    r = client.get("/tester-app")
    body = r.text
    assert "tailwind" in body.lower()
    assert "alpinejs" in body.lower() or "alpine" in body.lower()
    assert "JetBrains Mono" in body
    assert "DM Sans" in body
    assert "bg-slate-950" in body
