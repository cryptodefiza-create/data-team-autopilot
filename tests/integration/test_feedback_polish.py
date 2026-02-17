"""Tests for feedback system polish: enriched payloads, review, resolve, provider summary."""
from __future__ import annotations

from fastapi.testclient import TestClient

from data_autopilot.main import app

client = TestClient(app)

ADMIN_HEADERS = {
    "Content-Type": "application/json",
    "X-Tenant-Id": "org_test",
    "X-User-Role": "admin",
}
MEMBER_HEADERS = {
    "Content-Type": "application/json",
    "X-Tenant-Id": "org_test",
    "X-User-Role": "member",
}


def _create_feedback(
    feedback_type: str = "negative",
    artifact_type: str = "memo",
    provider: str | None = None,
    model: str | None = None,
    **extra,
) -> dict:
    payload = {
        "tenant_id": "org_test",
        "user_id": "tester_1",
        "artifact_id": "art_001",
        "artifact_version": 1,
        "artifact_type": artifact_type,
        "feedback_type": feedback_type,
        "provider": provider,
        "model": model,
        **extra,
    }
    r = client.post("/api/v1/feedback", json=payload, headers=ADMIN_HEADERS)
    assert r.status_code == 200, r.text
    return r.json()


# ---- 1. Enriched payload creates successfully ----
def test_enriched_feedback_payload() -> None:
    data = _create_feedback(
        provider="xai_grok",
        model="grok-3",
        session_id="sess_123",
        was_fallback=False,
        conversation_context=[{"role": "user", "text": "hello"}],
        channel="tester_app",
    )
    assert data["id"].startswith("fb_")
    assert "created_at" in data


# ---- 2. "chat" artifact_type accepted ----
def test_chat_artifact_type_accepted() -> None:
    data = _create_feedback(artifact_type="chat", provider="openai")
    assert data["id"].startswith("fb_")


# ---- 3. Review list returns unresolved items ----
def test_review_list_returns_items() -> None:
    _create_feedback(provider="xai_grok")
    r = client.get("/api/v1/feedback/review?org_id=org_test", headers=ADMIN_HEADERS)
    assert r.status_code == 200
    data = r.json()
    assert data["count"] >= 1
    assert any(item["resolved"] is False for item in data["items"])


# ---- 4. Review filters by provider ----
def test_review_filters_by_provider() -> None:
    _create_feedback(provider="filter_test_provider")
    r = client.get(
        "/api/v1/feedback/review?org_id=org_test&provider=filter_test_provider",
        headers=ADMIN_HEADERS,
    )
    assert r.status_code == 200
    data = r.json()
    assert data["count"] >= 1
    assert all(item["provider"] == "filter_test_provider" for item in data["items"])


# ---- 5. Review requires admin (403 for member) ----
def test_review_requires_admin() -> None:
    r = client.get("/api/v1/feedback/review?org_id=org_test", headers=MEMBER_HEADERS)
    assert r.status_code == 403


# ---- 6. Resolve marks as resolved ----
def test_resolve_marks_resolved() -> None:
    fb = _create_feedback(provider="xai_grok")
    fb_id = fb["id"]
    r = client.post(
        f"/api/v1/feedback/{fb_id}/resolve",
        json={"resolved_by": "admin_user"},
        headers=ADMIN_HEADERS,
    )
    assert r.status_code == 200
    data = r.json()
    assert data["resolved"] is True
    assert data["resolved_at"] is not None


# ---- 7. Resolve requires admin ----
def test_resolve_requires_admin() -> None:
    fb = _create_feedback(provider="xai_grok")
    r = client.post(
        f"/api/v1/feedback/{fb['id']}/resolve",
        json={"resolved_by": "member_user"},
        headers=MEMBER_HEADERS,
    )
    assert r.status_code == 403


# ---- 8. Resolve nonexistent returns 404 ----
def test_resolve_nonexistent_404() -> None:
    r = client.post(
        "/api/v1/feedback/fb_nonexistent/resolve",
        json={"resolved_by": "admin_user"},
        headers=ADMIN_HEADERS,
    )
    assert r.status_code == 404


# ---- 9. Provider summary returns by_provider/by_task with satisfaction_rate ----
def test_provider_summary_structure() -> None:
    _create_feedback(feedback_type="positive", provider="test_provider_a")
    _create_feedback(feedback_type="negative", provider="test_provider_a")
    r = client.get(
        "/api/v1/feedback/provider-summary?org_id=org_test", headers=ADMIN_HEADERS
    )
    assert r.status_code == 200
    data = r.json()
    assert "by_provider" in data
    assert "by_task" in data
    if "test_provider_a" in data["by_provider"]:
        entry = data["by_provider"]["test_provider_a"]
        assert "satisfaction_rate" in entry


# ---- 10. Satisfaction_rate math verified ----
def test_satisfaction_rate_math() -> None:
    # Create a unique provider so we can verify exact math
    _create_feedback(feedback_type="positive", provider="math_check_provider")
    _create_feedback(feedback_type="positive", provider="math_check_provider")
    _create_feedback(feedback_type="negative", provider="math_check_provider")
    r = client.get(
        "/api/v1/feedback/provider-summary?org_id=org_test", headers=ADMIN_HEADERS
    )
    data = r.json()
    entry = data["by_provider"]["math_check_provider"]
    # 2 positive / 3 total = 0.6667
    assert entry["positive"] >= 2
    assert entry["negative"] >= 1
    total = entry["positive"] + entry["negative"]
    expected_rate = round(entry["positive"] / total, 4)
    assert entry["satisfaction_rate"] == expected_rate
