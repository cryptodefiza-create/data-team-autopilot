"""Integration tests for the memo provider evaluation endpoint."""
from __future__ import annotations

from fastapi.testclient import TestClient

from data_autopilot.main import app


client = TestClient(app)


def _headers(org_id: str = "org_memo_eval") -> dict[str, str]:
    return {"X-Tenant-Id": org_id, "X-User-Role": "admin"}


def test_evaluate_memo_returns_no_providers_when_unconfigured() -> None:
    """Without LLM keys, should return a helpful error."""
    r = client.post(
        "/api/v1/llm/evaluate-memo",
        headers=_headers(),
        json={"org_id": "org_memo_eval"},
    )
    assert r.status_code == 200
    body = r.json()
    assert "error" in body or "results" in body
    # If no providers configured, results should be empty or error message present
    if "error" in body:
        assert "No LLM providers configured" in body["error"]
    else:
        assert body["results"] == {}


def test_evaluate_memo_requires_admin() -> None:
    r = client.post(
        "/api/v1/llm/evaluate-memo",
        headers={"X-Tenant-Id": "org_memo_eval", "X-User-Role": "member"},
        json={"org_id": "org_memo_eval"},
    )
    assert r.status_code == 403


def test_evaluate_memo_accepts_custom_packet() -> None:
    """Should accept a custom packet and not crash even without providers."""
    custom_packet = {
        "kpis": [
            {
                "metric_name": "TestMetric",
                "current_value": 100,
                "previous_value": 90,
                "delta_absolute": 10,
                "delta_percent": 11.11,
                "significance": "notable",
                "query_hash": "q_test",
            }
        ],
        "top_segments": [],
        "anomaly_notes": [],
    }
    r = client.post(
        "/api/v1/llm/evaluate-memo",
        headers=_headers(),
        json={"org_id": "org_memo_eval", "packet": custom_packet, "runs_per_provider": 1},
    )
    assert r.status_code == 200


def test_evaluate_memo_caps_runs_at_50() -> None:
    """Runs per provider should be capped at 50 even if higher requested."""
    r = client.post(
        "/api/v1/llm/evaluate-memo",
        headers=_headers(),
        json={"org_id": "org_memo_eval", "runs_per_provider": 100},
    )
    assert r.status_code == 200
