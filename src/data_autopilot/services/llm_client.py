from __future__ import annotations

import json
import time
from dataclasses import dataclass

import httpx

from data_autopilot.config.settings import get_settings


@dataclass(frozen=True)
class LLMProvider:
    """Configuration for a single LLM provider endpoint."""

    name: str
    base_url: str
    api_key: str
    model: str
    timeout_seconds: int = 30
    temperature: float = 0.0
    enabled: bool = True


@dataclass
class LLMResult:
    """Result from a single LLM call, including metadata for comparison."""

    provider_name: str
    model: str
    content: dict
    latency_ms: float
    input_tokens: int = 0
    output_tokens: int = 0
    error: str | None = None

    @property
    def succeeded(self) -> bool:
        return self.error is None


def _call_provider(provider: LLMProvider, system_prompt: str, user_prompt: str) -> LLMResult:
    """Execute a single LLM call against one provider. Never raises."""
    start = time.perf_counter()
    try:
        base = provider.base_url.rstrip("/")
        url = f"{base}/chat/completions"
        payload = {
            "model": provider.model,
            "temperature": provider.temperature,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        headers = {
            "Authorization": f"Bearer {provider.api_key}",
            "Content-Type": "application/json",
        }

        with httpx.Client(timeout=provider.timeout_seconds, follow_redirects=True) as client:
            response = client.post(url, headers=headers, json=payload)
            response.raise_for_status()
            body = response.json()

        choices = body.get("choices")
        if not isinstance(choices, list) or not choices:
            raise RuntimeError("LLM response missing choices")

        message = choices[0].get("message", {})
        raw_content = message.get("content", "")
        if isinstance(raw_content, list):
            raw_content = "".join(
                str(item.get("text", "")) for item in raw_content if isinstance(item, dict)
            )
        if not isinstance(raw_content, str) or not raw_content.strip():
            raise RuntimeError("LLM response content is empty")

        parsed = json.loads(raw_content)
        if not isinstance(parsed, dict):
            raise RuntimeError("LLM JSON response must be an object")

        usage = body.get("usage", {})
        latency_ms = (time.perf_counter() - start) * 1000

        return LLMResult(
            provider_name=provider.name,
            model=provider.model,
            content=parsed,
            latency_ms=round(latency_ms, 2),
            input_tokens=int(usage.get("prompt_tokens", 0)),
            output_tokens=int(usage.get("completion_tokens", 0)),
        )
    except Exception as exc:
        latency_ms = (time.perf_counter() - start) * 1000
        return LLMResult(
            provider_name=provider.name,
            model=provider.model,
            content={},
            latency_ms=round(latency_ms, 2),
            error=str(exc),
        )


class LLMClient:
    """OpenAI-compatible LLM client.

    Supports a primary provider (from settings) and optional override provider
    for testing or multi-provider routing.
    """

    def __init__(self, provider: LLMProvider | None = None) -> None:
        self.settings = get_settings()
        self._provider = provider

    @property
    def provider(self) -> LLMProvider | None:
        if self._provider is not None:
            return self._provider
        if self.settings.llm_api_key and self.settings.llm_model:
            return LLMProvider(
                name="primary",
                base_url=self.settings.llm_api_base_url,
                api_key=self.settings.llm_api_key,
                model=self.settings.llm_model,
                timeout_seconds=self.settings.llm_timeout_seconds,
                temperature=self.settings.llm_temperature,
            )
        return None

    def is_configured(self) -> bool:
        return self.provider is not None

    def generate_json(self, system_prompt: str, user_prompt: str) -> dict:
        p = self.provider
        if p is None:
            raise RuntimeError("LLM is not configured")

        result = _call_provider(p, system_prompt, user_prompt)
        if not result.succeeded:
            raise RuntimeError(result.error)
        return result.content

    def generate_json_with_meta(self, system_prompt: str, user_prompt: str) -> LLMResult:
        """Like generate_json but returns the full LLMResult with metadata."""
        p = self.provider
        if p is None:
            raise RuntimeError("LLM is not configured")
        return _call_provider(p, system_prompt, user_prompt)


def get_eval_providers() -> list[LLMProvider]:
    """Build evaluation providers from dedicated env vars + legacy JSON override."""
    settings = get_settings()
    if not settings.llm_eval_enabled:
        return []

    providers: list[LLMProvider] = []

    # --- Dedicated env-var providers (preferred for Railway / simple deploys) ---
    if settings.gpt5_mini_enabled and settings.gpt5_mini_api_key:
        providers.append(
            LLMProvider(
                name="gpt5_mini",
                base_url=settings.gpt5_mini_base_url,
                api_key=settings.gpt5_mini_api_key,
                model=settings.gpt5_mini_model,
            )
        )

    if settings.claude_sonnet_enabled and settings.claude_sonnet_api_key:
        providers.append(
            LLMProvider(
                name="claude_sonnet",
                base_url=settings.claude_sonnet_base_url,
                api_key=settings.claude_sonnet_api_key,
                model=settings.claude_sonnet_model,
            )
        )

    # --- Legacy JSON override (additional providers beyond the two built-in) ---
    try:
        raw = json.loads(settings.llm_eval_providers_json)
    except (json.JSONDecodeError, TypeError):
        raw = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        if not entry.get("enabled", True):
            continue
        try:
            providers.append(
                LLMProvider(
                    name=str(entry["name"]),
                    base_url=str(entry["base_url"]),
                    api_key=str(entry["api_key"]),
                    model=str(entry["model"]),
                    timeout_seconds=int(entry.get("timeout_seconds", 30)),
                    temperature=float(entry.get("temperature", 0.0)),
                )
            )
        except (KeyError, ValueError):
            continue

    return providers
