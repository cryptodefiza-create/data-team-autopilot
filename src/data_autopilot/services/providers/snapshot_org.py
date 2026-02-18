from __future__ import annotations

import logging
from typing import Any

from data_autopilot.services.mode1.models import ProviderResult
from data_autopilot.services.providers.base import BaseProvider

logger = logging.getLogger(__name__)

_SNAPSHOT_API = "https://hub.snapshot.org/graphql"


class SnapshotProvider(BaseProvider):
    """Snapshot.org governance data provider."""

    name = "snapshot"

    def __init__(self, mock_mode: bool = False) -> None:
        super().__init__(api_key="", base_url=_SNAPSHOT_API)
        self._mock_mode = mock_mode
        self._mock_data: dict[str, Any] = {}

    def register_mock_data(self, method: str, data: Any) -> None:
        self._mock_data[method] = data

    def fetch(self, method: str, params: dict[str, Any]) -> ProviderResult:
        return self._dispatch_fetch(method, params, {
            "get_proposals": self._get_proposals,
            "get_votes": self._get_votes,
        })

    def _get_proposals(self, params: dict[str, Any]) -> ProviderResult:
        space = params.get("space", "")
        limit = params.get("limit", 20)
        if self._mock_mode:
            data = self._mock_data.get("get_proposals", [
                {
                    "id": f"proposal_{i}",
                    "title": f"Proposal #{i}: Budget Allocation Q{(i % 4) + 1}",
                    "state": "active" if i < 2 else "closed",
                    "scores_total": 100_000 + i * 25_000,
                    "votes": 150 + i * 30,
                    "start": 1700000000 + i * 86400,
                    "end": 1700000000 + (i + 7) * 86400,
                    "space": {"id": space},
                }
                for i in range(min(limit, 10))
            ])
            return ProviderResult(
                provider=self.name, method="get_proposals",
                records=data, total_available=len(data),
            )

        query = (
            '{ proposals(first: %d, skip: 0, where: { space_in: ["%s"] }, '
            'orderBy: "created", orderDirection: desc) '
            "{ id title state scores_total votes start end space { id } } }"
        ) % (limit, space)

        resp = self._client.post(self.base_url, json={"query": query})
        resp.raise_for_status()
        body = resp.json()
        proposals = body.get("data", {}).get("proposals", [])
        return ProviderResult(
            provider=self.name, method="get_proposals",
            records=proposals, total_available=len(proposals),
        )

    def _get_votes(self, params: dict[str, Any]) -> ProviderResult:
        proposal_id = params.get("proposal_id", "")
        if self._mock_mode:
            data = self._mock_data.get("get_votes", [
                {"voter": f"0xvoter{i}", "choice": 1 if i % 3 != 0 else 2,
                 "vp": 1000 + i * 100}
                for i in range(50)
            ])
            return ProviderResult(
                provider=self.name, method="get_votes",
                records=data, total_available=len(data),
            )

        query = (
            '{ votes(first: 1000, where: { proposal: "%s" }) '
            "{ voter choice vp } }"
        ) % proposal_id

        resp = self._client.post(self.base_url, json={"query": query})
        resp.raise_for_status()
        body = resp.json()
        votes = body.get("data", {}).get("votes", [])
        return ProviderResult(
            provider=self.name, method="get_votes",
            records=votes, total_available=len(votes),
        )
