"""Integration tests for LLM cost tracking API endpoints."""
from __future__ import annotations

from fastapi.testclient import TestClient

from data_autopilot.main import app


client = TestClient(app)


def _headers(org_id: str = "org_cost") -> dict[str, str]:
    return {"X-Tenant-Id": org_id, "X-User-Role": "admin"}


def test_llm_usage_endpoint_empty() -> None:
    r = client.get(
        "/api/v1/llm/usage",
        params={"org_id": "org_cost_empty"},
        headers={"X-Tenant-Id": "org_cost_empty", "X-User-Role": "member"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["tenant_id"] == "org_cost_empty"
    assert body["total_requests"] == 0
    assert body["total_estimated_cost_usd"] == 0
    assert body["by_provider"] == {}


def test_llm_budget_endpoint() -> None:
    r = client.get(
        "/api/v1/llm/budget",
        params={"org_id": "org_cost_budget"},
        headers={"X-Tenant-Id": "org_cost_budget", "X-User-Role": "member"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["tenant_id"] == "org_cost_budget"
    assert body["budget_usd"] == 100.0
    assert body["over_budget"] is False
    assert body["usage_pct"] == 0.0


def test_cost_service_records_and_retrieves() -> None:
    """Record LLM usage and verify it shows in the usage summary."""
    from data_autopilot.db.session import SessionLocal
    from data_autopilot.services.llm_client import LLMResult
    from data_autopilot.services.llm_cost_service import LLMCostService

    db = SessionLocal()
    try:
        svc = LLMCostService()
        result = LLMResult(
            provider_name="primary",
            model="grok-4-fast",
            content={"action": "query"},
            latency_ms=150.0,
            input_tokens=500,
            output_tokens=200,
        )
        svc.record(db, tenant_id="org_cost_track", result=result, task_type="intent_classification")

        # Verify via API
        r = client.get(
            "/api/v1/llm/usage",
            params={"org_id": "org_cost_track"},
            headers={"X-Tenant-Id": "org_cost_track", "X-User-Role": "member"},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["total_requests"] >= 1
        assert body["total_input_tokens"] >= 500
        assert body["total_output_tokens"] >= 200
        assert body["total_estimated_cost_usd"] > 0
        assert "primary" in body["by_provider"]
        assert body["by_provider"]["primary"]["model"] == "grok-4-fast"
    finally:
        db.close()


def test_budget_reflects_recorded_usage() -> None:
    """After recording usage, budget should reflect the cost."""
    from data_autopilot.db.session import SessionLocal
    from data_autopilot.services.llm_client import LLMResult
    from data_autopilot.services.llm_cost_service import LLMCostService

    db = SessionLocal()
    try:
        svc = LLMCostService()
        # Record a large usage
        result = LLMResult(
            provider_name="expensive",
            model="claude-sonnet-4-5-20250929",
            content={},
            latency_ms=500.0,
            input_tokens=100_000,
            output_tokens=50_000,
        )
        svc.record(db, tenant_id="org_budget_test", result=result, task_type="memo_generation")

        r = client.get(
            "/api/v1/llm/budget",
            params={"org_id": "org_budget_test"},
            headers={"X-Tenant-Id": "org_budget_test", "X-User-Role": "member"},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["total_cost_usd"] > 0
        assert body["remaining_usd"] < body["budget_usd"]
    finally:
        db.close()


def test_check_budget_returns_true_when_within() -> None:
    from data_autopilot.db.session import SessionLocal
    from data_autopilot.services.llm_cost_service import LLMCostService

    db = SessionLocal()
    try:
        svc = LLMCostService()
        assert svc.check_budget(db, tenant_id="org_fresh") is True
    finally:
        db.close()
