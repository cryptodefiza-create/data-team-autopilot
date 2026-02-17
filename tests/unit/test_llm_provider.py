"""Unit tests for multi-provider LLM infrastructure."""
from __future__ import annotations

from data_autopilot.services.llm_client import (
    LLMClient,
    LLMProvider,
    LLMResult,
    _call_provider,
    get_eval_providers,
)


def test_llm_provider_dataclass() -> None:
    p = LLMProvider(
        name="grok",
        base_url="https://api.x.ai/v1",
        api_key="xai-key",
        model="grok-4-fast",
    )
    assert p.name == "grok"
    assert p.enabled is True
    assert p.timeout_seconds == 30
    assert p.temperature == 0.0


def test_llm_result_succeeded() -> None:
    r = LLMResult(
        provider_name="test",
        model="test-model",
        content={"action": "query"},
        latency_ms=42.0,
    )
    assert r.succeeded is True
    assert r.error is None


def test_llm_result_failed() -> None:
    r = LLMResult(
        provider_name="test",
        model="test-model",
        content={},
        latency_ms=100.0,
        error="Connection refused",
    )
    assert r.succeeded is False
    assert r.error == "Connection refused"


def test_call_provider_returns_error_on_bad_url() -> None:
    p = LLMProvider(
        name="bad",
        base_url="http://127.0.0.1:1",
        api_key="fake",
        model="fake-model",
        timeout_seconds=1,
    )
    result = _call_provider(p, "system", "user")
    assert not result.succeeded
    assert result.error
    assert result.latency_ms >= 0


def test_llm_client_not_configured_by_default() -> None:
    client = LLMClient()
    assert not client.is_configured()


def test_llm_client_with_explicit_provider() -> None:
    p = LLMProvider(
        name="test",
        base_url="http://localhost:9999",
        api_key="key",
        model="model",
    )
    client = LLMClient(provider=p)
    assert client.is_configured()
    assert client.provider == p


def test_get_eval_providers_empty_by_default() -> None:
    providers = get_eval_providers()
    assert providers == []
