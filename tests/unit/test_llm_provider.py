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


def test_get_eval_providers_from_dedicated_env_vars(monkeypatch) -> None:
    """Dedicated env vars (GPT5_MINI_*, CLAUDE_SONNET_*) produce providers."""
    from data_autopilot.config.settings import get_settings

    settings = get_settings()
    monkeypatch.setattr(settings, "llm_eval_enabled", True)
    monkeypatch.setattr(settings, "gpt5_mini_enabled", True)
    monkeypatch.setattr(settings, "gpt5_mini_api_key", "sk-test-gpt5")
    monkeypatch.setattr(settings, "gpt5_mini_model", "gpt-5-mini")
    monkeypatch.setattr(settings, "gpt5_mini_base_url", "https://api.openai.com/v1")
    monkeypatch.setattr(settings, "claude_sonnet_enabled", True)
    monkeypatch.setattr(settings, "claude_sonnet_api_key", "sk-test-claude")
    monkeypatch.setattr(settings, "claude_sonnet_model", "claude-sonnet-4-5-20250929")
    monkeypatch.setattr(settings, "claude_sonnet_base_url", "https://api.anthropic.com/v1")
    monkeypatch.setattr(settings, "llm_eval_providers_json", "[]")

    providers = get_eval_providers()
    assert len(providers) == 2
    names = [p.name for p in providers]
    assert "gpt5_mini" in names
    assert "claude_sonnet" in names

    gpt = next(p for p in providers if p.name == "gpt5_mini")
    assert gpt.api_key == "sk-test-gpt5"
    assert gpt.model == "gpt-5-mini"

    claude = next(p for p in providers if p.name == "claude_sonnet")
    assert claude.api_key == "sk-test-claude"
    assert claude.model == "claude-sonnet-4-5-20250929"


def test_get_eval_providers_skips_disabled(monkeypatch) -> None:
    """Disabled dedicated providers are not included."""
    from data_autopilot.config.settings import get_settings

    settings = get_settings()
    monkeypatch.setattr(settings, "llm_eval_enabled", True)
    monkeypatch.setattr(settings, "gpt5_mini_enabled", False)
    monkeypatch.setattr(settings, "gpt5_mini_api_key", "sk-test")
    monkeypatch.setattr(settings, "claude_sonnet_enabled", True)
    monkeypatch.setattr(settings, "claude_sonnet_api_key", "")  # no key
    monkeypatch.setattr(settings, "llm_eval_providers_json", "[]")

    providers = get_eval_providers()
    assert len(providers) == 0


def test_get_eval_providers_combines_dedicated_and_json(monkeypatch) -> None:
    """Dedicated providers + JSON providers are merged."""
    import json
    from data_autopilot.config.settings import get_settings

    settings = get_settings()
    monkeypatch.setattr(settings, "llm_eval_enabled", True)
    monkeypatch.setattr(settings, "gpt5_mini_enabled", True)
    monkeypatch.setattr(settings, "gpt5_mini_api_key", "sk-gpt5")
    monkeypatch.setattr(settings, "llm_eval_providers_json", json.dumps([
        {"name": "custom", "base_url": "http://localhost:8000", "api_key": "k", "model": "m"},
    ]))

    providers = get_eval_providers()
    names = [p.name for p in providers]
    assert "gpt5_mini" in names
    assert "custom" in names
