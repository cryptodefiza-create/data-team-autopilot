from __future__ import annotations

import logging
import time

logger = logging.getLogger(__name__)

_COOLDOWN_SECONDS = 60.0


class PlatformKeyManager:
    """Round-robin API key pool with rate-limit cooldown tracking per provider."""

    def __init__(self) -> None:
        self._pools: dict[str, list[str]] = {}
        self._indices: dict[str, int] = {}
        self._cooldowns: dict[str, float] = {}  # "provider:key" → cooldown_until

    def register(self, provider: str, keys: list[str]) -> None:
        valid = [k for k in keys if k]
        if valid:
            self._pools[provider] = valid
            self._indices[provider] = 0

    def acquire(self, provider: str) -> str | None:
        keys = self._pools.get(provider, [])
        if not keys:
            return None
        now = time.monotonic()
        start_idx = self._indices.get(provider, 0)
        for i in range(len(keys)):
            idx = (start_idx + i) % len(keys)
            key = keys[idx]
            cooldown_key = f"{provider}:{key}"
            if self._cooldowns.get(cooldown_key, 0) <= now:
                self._indices[provider] = (idx + 1) % len(keys)
                return key
        logger.warning("All keys for %s are rate-limited", provider)
        return None

    def mark_rate_limited(self, provider: str, key: str) -> None:
        cooldown_key = f"{provider}:{key}"
        self._cooldowns[cooldown_key] = time.monotonic() + _COOLDOWN_SECONDS
        logger.info("Key for %s marked rate-limited for %ss", provider, _COOLDOWN_SECONDS)
