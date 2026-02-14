from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from data_autopilot.config.settings import get_settings
from data_autopilot.services.redis_store import RedisStore


@dataclass
class CacheResult:
    value: dict | None
    cache_hit: bool


class CacheService:
    def __init__(self, store: RedisStore | None = None) -> None:
        settings = get_settings()
        self.store = store or RedisStore(settings.redis_url)
        self.settings = settings

    def get(self, key: str) -> CacheResult:
        value = self.store.get_json(key)
        return CacheResult(value=value, cache_hit=value is not None)

    def set(self, key: str, value: dict, ttl_seconds: Optional[int] = None) -> None:
        self.store.set_json(key, value, ttl_seconds=ttl_seconds)

    def invalidate_connection(self, connection_id: str) -> int:
        return self.store.delete_prefix(f"schema:{connection_id}") + self.store.delete_prefix(f"query:{connection_id}")
