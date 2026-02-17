"""Unit tests for LLM cost tracking and budget management."""
from __future__ import annotations

from data_autopilot.services.llm_cost_service import (
    PROVIDER_RATES,
    estimate_cost_usd,
)


def test_estimate_cost_grok() -> None:
    # 1000 input tokens, 500 output tokens at grok-4-fast rates
    cost = estimate_cost_usd("grok-4-fast", 1000, 500)
    expected = (1000 / 1_000_000) * 0.60 + (500 / 1_000_000) * 2.40
    assert cost == round(expected, 6)


def test_estimate_cost_gpt5_mini() -> None:
    cost = estimate_cost_usd("gpt-5-mini", 1000, 500)
    expected = (1000 / 1_000_000) * 1.50 + (500 / 1_000_000) * 6.00
    assert cost == round(expected, 6)


def test_estimate_cost_claude_sonnet() -> None:
    cost = estimate_cost_usd("claude-sonnet-4-5-20250929", 1000, 500)
    expected = (1000 / 1_000_000) * 3.00 + (500 / 1_000_000) * 15.00
    assert cost == round(expected, 6)


def test_estimate_cost_unknown_model_uses_default() -> None:
    cost = estimate_cost_usd("some-unknown-model", 1000, 500)
    expected = (1000 / 1_000_000) * 1.00 + (500 / 1_000_000) * 4.00
    assert cost == round(expected, 6)


def test_estimate_cost_zero_tokens() -> None:
    cost = estimate_cost_usd("grok-4-fast", 0, 0)
    assert cost == 0.0


def test_provider_rates_structure() -> None:
    for model, rates in PROVIDER_RATES.items():
        assert "input" in rates
        assert "output" in rates
        assert rates["input"] >= 0
        assert rates["output"] >= 0
