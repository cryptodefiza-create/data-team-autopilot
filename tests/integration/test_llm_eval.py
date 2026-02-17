"""Integration tests for LLM evaluation service and API endpoints."""
from __future__ import annotations

from fastapi.testclient import TestClient

from data_autopilot.main import app


client = TestClient(app)


def _headers(org_id: str = "org_eval") -> dict[str, str]:
    return {"X-Tenant-Id": org_id, "X-User-Role": "admin"}


def test_llm_status_shows_eval_fields() -> None:
    r = client.get("/api/v1/llm/status")
    assert r.status_code == 200
    body = r.json()
    assert "eval_enabled" in body
    assert "eval_providers" in body
    assert isinstance(body["eval_providers"], list)


def test_llm_status_eval_disabled_by_default() -> None:
    r = client.get("/api/v1/llm/status")
    body = r.json()
    assert body["eval_enabled"] is False
    assert body["eval_providers"] == []


def test_eval_runs_endpoint_empty() -> None:
    r = client.get(
        "/api/v1/llm/eval-runs",
        params={"org_id": "org_eval"},
        headers=_headers(),
    )
    assert r.status_code == 200
    body = r.json()
    assert body["org_id"] == "org_eval"
    assert body["count"] == 0
    assert body["items"] == []


def test_eval_runs_endpoint_with_task_type_filter() -> None:
    r = client.get(
        "/api/v1/llm/eval-runs",
        params={"org_id": "org_eval", "task_type": "intent_classification"},
        headers=_headers(),
    )
    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 0


def test_eval_service_stores_run_in_audit() -> None:
    """Verify that the eval service stores results via audit log."""
    from data_autopilot.db.session import SessionLocal
    from data_autopilot.services.llm_client import LLMResult
    from data_autopilot.services.llm_eval_service import EvalRun, _store_eval_run
    from datetime import datetime, timezone

    db = SessionLocal()
    try:
        run = EvalRun(
            run_id="eval_test123",
            tenant_id="org_eval_store",
            task_type="intent_classification",
            started_at=datetime.now(timezone.utc),
            primary=LLMResult(
                provider_name="primary",
                model="grok-4-fast",
                content={"action": "query"},
                latency_ms=150.0,
                input_tokens=100,
                output_tokens=50,
            ),
            evaluations=[
                LLMResult(
                    provider_name="gpt5-mini",
                    model="gpt-5-mini",
                    content={"action": "query"},
                    latency_ms=200.0,
                    input_tokens=100,
                    output_tokens=60,
                ),
                LLMResult(
                    provider_name="claude-sonnet",
                    model="claude-sonnet-4-5-20250929",
                    content={"action": "dashboard"},
                    latency_ms=180.0,
                    error="timeout",
                ),
            ],
        )
        _store_eval_run(db, run)

        # Verify via API
        r = client.get(
            "/api/v1/llm/eval-runs",
            params={"org_id": "org_eval_store"},
            headers={"X-Tenant-Id": "org_eval_store", "X-User-Role": "admin"},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["count"] >= 1
        # Find our specific run
        item = next(i for i in body["items"] if i["run_id"] == "eval_test123")
        assert item["task_type"] == "intent_classification"
        assert item["primary"]["provider_name"] == "primary"
        assert item["primary"]["model"] == "grok-4-fast"
        assert item["primary"]["succeeded"] is True
        assert len(item["evaluations"]) == 2
        assert item["evaluations"][0]["provider_name"] == "gpt5-mini"
        assert item["evaluations"][1]["succeeded"] is False
    finally:
        db.close()


def test_eval_run_to_dict() -> None:
    from data_autopilot.services.llm_client import LLMResult
    from data_autopilot.services.llm_eval_service import EvalRun
    from datetime import datetime, timezone

    run = EvalRun(
        run_id="eval_dict_test",
        tenant_id="org_x",
        task_type="memo_generation",
        started_at=datetime(2026, 2, 17, 12, 0, 0, tzinfo=timezone.utc),
        primary=LLMResult(
            provider_name="primary",
            model="grok-4-fast",
            content={"headline_summary": ["Revenue up 5%"]},
            latency_ms=300.0,
        ),
        evaluations=[],
    )
    d = run.to_dict()
    assert d["run_id"] == "eval_dict_test"
    assert d["task_type"] == "memo_generation"
    assert d["primary"]["content_keys"] == ["headline_summary"]
    assert d["evaluations"] == []
