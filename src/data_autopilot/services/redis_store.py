from __future__ import annotations

import json
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

try:
    import redis as redis_lib
except Exception:  # pragma: no cover
    redis_lib = None


@dataclass
class _InMemValue:
    value: str
    expires_at: float | None = None


class RedisStore:
    """Redis wrapper with in-memory fallback for local/test environments."""

    def __init__(self, redis_url: str) -> None:
        self._client = None
        self._kv: Dict[str, _InMemValue] = {}
        self._zsets: Dict[str, List[Tuple[float, float]]] = defaultdict(list)

        if redis_lib is not None:
            try:
                self._client = redis_lib.Redis.from_url(redis_url, decode_responses=True)
                self._client.ping()
            except Exception:
                self._client = None

    def _cleanup(self) -> None:
        now = time.time()
        for key in list(self._kv.keys()):
            exp = self._kv[key].expires_at
            if exp is not None and exp <= now:
                del self._kv[key]

    def get_json(self, key: str) -> Optional[dict]:
        if self._client is not None:
            value = self._client.get(key)
            return json.loads(value) if value else None
        self._cleanup()
        value = self._kv.get(key)
        return json.loads(value.value) if value else None

    def set_json(self, key: str, value: dict, ttl_seconds: int | None = None) -> None:
        encoded = json.dumps(value)
        if self._client is not None:
            if ttl_seconds:
                self._client.setex(key, ttl_seconds, encoded)
            else:
                self._client.set(key, encoded)
            return
        exp = time.time() + ttl_seconds if ttl_seconds else None
        self._kv[key] = _InMemValue(value=encoded, expires_at=exp)

    def delete_prefix(self, prefix: str) -> int:
        if self._client is not None:
            keys = self._client.keys(f"{prefix}*")
            return self._client.delete(*keys) if keys else 0

        self._cleanup()
        keys = [k for k in self._kv if k.startswith(prefix)]
        for key in keys:
            del self._kv[key]
        return len(keys)

    def count_prefix(self, prefix: str) -> int:
        if self._client is not None:
            keys = self._client.keys(f"{prefix}*")
            return len(keys)
        self._cleanup()
        return sum(1 for key in self._kv if key.startswith(prefix))

    def zadd(self, key: str, score: float, value: float) -> None:
        if self._client is not None:
            self._client.zadd(key, {str(value): score})
            return
        self._zsets[key].append((score, value))

    def zremrangebyscore(self, key: str, min_score: float, max_score: float) -> None:
        if self._client is not None:
            self._client.zremrangebyscore(key, min_score, max_score)
            return
        self._zsets[key] = [(s, v) for s, v in self._zsets.get(key, []) if not (min_score <= s <= max_score)]

    def zrangebyscore(self, key: str, min_score: float, max_score: float) -> List[Tuple[float, float]]:
        if self._client is not None:
            rows = self._client.zrangebyscore(key, min_score, max_score, withscores=True)
            return [(score, float(value)) for value, score in rows]
        return [(s, v) for s, v in self._zsets.get(key, []) if min_score <= s <= max_score]
