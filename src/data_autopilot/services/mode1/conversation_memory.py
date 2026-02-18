from __future__ import annotations

from typing import Any

from data_autopilot.services.mode1.models import ConversationTurn, DataRequest


class ConversationMemory:
    """Stores recent data results for follow-up queries."""

    def __init__(self, max_turns: int = 10) -> None:
        self._history: list[ConversationTurn] = []
        self._max_turns = max_turns

    def store_result(self, request: DataRequest, data: list[dict[str, Any]]) -> None:
        self._history.append(ConversationTurn(request=request, data=data))
        if len(self._history) > self._max_turns:
            self._history = self._history[-self._max_turns:]

    def get_context(self) -> dict[str, Any]:
        """Returns metadata about the last query for LLM context."""
        if not self._history:
            return {"has_previous_data": False}
        last = self._history[-1]
        return {
            "previous_query": last.request.raw_message,
            "previous_entity": last.request.entity.value,
            "previous_chain": last.request.chain.value,
            "previous_token": last.request.token,
            "previous_record_count": len(last.data),
            "previous_columns": list(last.data[0].keys()) if last.data else [],
            "has_previous_data": True,
        }

    def get_previous_data(self) -> list[dict[str, Any]] | None:
        """Returns the actual data from last query for filtering/re-export."""
        if self._history:
            return self._history[-1].data
        return None

    def get_previous_request(self) -> DataRequest | None:
        if self._history:
            return self._history[-1].request
        return None

    @property
    def has_history(self) -> bool:
        return len(self._history) > 0

    def clear(self) -> None:
        self._history.clear()
