from __future__ import annotations

import hashlib
import hmac
import json
import re
import time

import httpx

from data_autopilot.config.settings import get_settings
from data_autopilot.services.redis_store import RedisStore


class ChannelIntegrationsService:
    def __init__(self, http_client: httpx.Client | None = None) -> None:
        self.settings = get_settings()
        self.redis = RedisStore(self.settings.redis_url)
        self.http_client = http_client or httpx.Client(timeout=10)

    def verify_slack_signature(self, raw_body: bytes, timestamp: str, signature: str) -> bool:
        if not self.settings.slack_signing_secret:
            return False
        if not timestamp or not signature:
            return False
        try:
            ts = int(timestamp)
        except ValueError:
            return False
        now = int(time.time())
        if abs(now - ts) > 300:
            return False
        base = f"v0:{timestamp}:{raw_body.decode('utf-8')}".encode("utf-8")
        digest = hmac.new(
            self.settings.slack_signing_secret.encode("utf-8"),
            base,
            hashlib.sha256,
        ).hexdigest()
        expected = f"v0={digest}"
        if not hmac.compare_digest(expected, signature):
            return False
        replay_key = f"slack:replay:{timestamp}:{signature}"
        return self.redis.set_once(replay_key, ttl_seconds=300)

    def verify_telegram_secret(self, secret_header: str) -> bool:
        configured = self.settings.telegram_webhook_secret
        if not configured:
            return False
        return hmac.compare_digest(configured, secret_header or "")

    @staticmethod
    def _extract_requested_org_and_prompt(text: str) -> tuple[str | None, str]:
        clean = text.strip()
        clean = re.sub(r"<@[^>]+>", "", clean).strip()
        if clean.startswith("/ask"):
            parts = clean.split(maxsplit=1)
            clean = parts[1].strip() if len(parts) > 1 else ""
        org_id: str | None = None
        if clean.startswith("org:"):
            first, *rest = clean.split(maxsplit=1)
            org_id = first.split(":", 1)[1].strip()
            clean = rest[0].strip() if rest else ""
        return org_id, clean

    def parse_slack_message(self, text: str) -> tuple[str | None, str]:
        return self._extract_requested_org_and_prompt(text)

    def parse_telegram_message(self, text: str) -> tuple[str | None, str]:
        return self._extract_requested_org_and_prompt(text)

    @staticmethod
    def format_agent_result(result: dict) -> str:
        summary = str(result.get("summary", ""))
        data = result.get("data", {})
        rows = data.get("rows", []) if isinstance(data, dict) else []
        if rows:
            return f"{summary}\nRows: {len(rows)}"
        reasons = data.get("reasons", []) if isinstance(data, dict) else []
        if reasons:
            return f"{summary}\nReason: {', '.join(str(r) for r in reasons)}"
        return summary or "Completed."

    def send_slack_message(self, channel: str, text: str, thread_ts: str | None = None) -> None:
        if not self.settings.slack_bot_token:
            return
        payload: dict[str, str] = {"channel": channel, "text": text}
        if thread_ts:
            payload["thread_ts"] = thread_ts
        self.http_client.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {self.settings.slack_bot_token}"},
            json=payload,
        )

    def send_telegram_message(self, chat_id: str, text: str) -> None:
        if not self.settings.telegram_bot_token:
            return
        url = f"https://api.telegram.org/bot{self.settings.telegram_bot_token}/sendMessage"
        self.http_client.post(url, json={"chat_id": chat_id, "text": text})

    @staticmethod
    def parse_slack_event(raw_json: bytes) -> dict:
        return json.loads(raw_json.decode("utf-8"))
