from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

logger = logging.getLogger(__name__)


class FeedbackEntry:
    """A single feedback submission."""

    def __init__(
        self,
        org_id: str,
        user_id: str,
        rating: str,  # "up" | "down"
        provider: str = "",
        query_type: str = "",
        mode: str = "",
        tier: str = "",
        message: str = "",
        query_text: str = "",
    ) -> None:
        self.id = f"fb_{uuid4().hex[:10]}"
        self.org_id = org_id
        self.user_id = user_id
        self.rating = rating
        self.provider = provider
        self.query_type = query_type
        self.mode = mode
        self.tier = tier
        self.message = message
        self.query_text = query_text
        self.created_at = datetime.now(timezone.utc)


class FeedbackSystem:
    """Collects and analyzes thumbs up/down feedback on agent responses."""

    def __init__(self) -> None:
        self._entries: list[FeedbackEntry] = []

    def submit(
        self,
        org_id: str,
        user_id: str,
        rating: str,
        provider: str = "",
        query_type: str = "",
        mode: str = "",
        tier: str = "",
        message: str = "",
        query_text: str = "",
    ) -> FeedbackEntry:
        """Submit feedback for a response."""
        if rating not in ("up", "down"):
            raise ValueError(f"Rating must be 'up' or 'down', got '{rating}'")

        entry = FeedbackEntry(
            org_id=org_id,
            user_id=user_id,
            rating=rating,
            provider=provider,
            query_type=query_type,
            mode=mode,
            tier=tier,
            message=message,
            query_text=query_text,
        )
        self._entries.append(entry)
        logger.info(
            "Feedback %s from %s/%s: %s (provider=%s, mode=%s)",
            entry.id, org_id, user_id, rating, provider, mode,
        )
        return entry

    def get_feedback(
        self,
        org_id: str | None = None,
        provider: str | None = None,
        mode: str | None = None,
        rating: str | None = None,
    ) -> list[FeedbackEntry]:
        """Retrieve feedback, optionally filtered."""
        results = list(self._entries)
        if org_id:
            results = [e for e in results if e.org_id == org_id]
        if provider:
            results = [e for e in results if e.provider == provider]
        if mode:
            results = [e for e in results if e.mode == mode]
        if rating:
            results = [e for e in results if e.rating == rating]
        return results

    def get_stats(
        self, org_id: str | None = None
    ) -> dict[str, Any]:
        """Get feedback statistics."""
        entries = self._entries
        if org_id:
            entries = [e for e in entries if e.org_id == org_id]

        total = len(entries)
        up = sum(1 for e in entries if e.rating == "up")
        down = total - up

        by_provider: dict[str, dict[str, int]] = {}
        for e in entries:
            if e.provider not in by_provider:
                by_provider[e.provider] = {"up": 0, "down": 0}
            by_provider[e.provider][e.rating] += 1

        by_mode: dict[str, dict[str, int]] = {}
        for e in entries:
            if e.mode not in by_mode:
                by_mode[e.mode] = {"up": 0, "down": 0}
            by_mode[e.mode][e.rating] += 1

        return {
            "total": total,
            "up": up,
            "down": down,
            "satisfaction_rate": up / total if total > 0 else 0.0,
            "by_provider": by_provider,
            "by_mode": by_mode,
        }

    @property
    def count(self) -> int:
        return len(self._entries)
