from __future__ import annotations

import json

import httpx

from data_autopilot.config.settings import get_settings


class LLMClient:
    def __init__(self) -> None:
        self.settings = get_settings()

    def is_configured(self) -> bool:
        return bool(self.settings.llm_api_key and self.settings.llm_model)

    def generate_json(self, system_prompt: str, user_prompt: str) -> dict:
        if not self.is_configured():
            raise RuntimeError("LLM is not configured")

        base = self.settings.llm_api_base_url.rstrip("/")
        url = f"{base}/chat/completions"
        payload = {
            "model": self.settings.llm_model,
            "temperature": self.settings.llm_temperature,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        headers = {
            "Authorization": f"Bearer {self.settings.llm_api_key}",
            "Content-Type": "application/json",
        }

        with httpx.Client(timeout=self.settings.llm_timeout_seconds, follow_redirects=True) as client:
            response = client.post(url, headers=headers, json=payload)
            response.raise_for_status()
            body = response.json()

        choices = body.get("choices")
        if not isinstance(choices, list) or not choices:
            raise RuntimeError("LLM response missing choices")

        message = choices[0].get("message", {})
        content = message.get("content", "")
        if isinstance(content, list):
            content = "".join(str(item.get("text", "")) for item in content if isinstance(item, dict))
        if not isinstance(content, str) or not content.strip():
            raise RuntimeError("LLM response content is empty")

        try:
            parsed = json.loads(content)
        except json.JSONDecodeError as exc:
            raise RuntimeError("LLM did not return valid JSON") from exc
        if not isinstance(parsed, dict):
            raise RuntimeError("LLM JSON response must be an object")
        return parsed
