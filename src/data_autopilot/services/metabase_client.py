from __future__ import annotations

import logging
from typing import Any
from uuid import uuid4

import httpx

from data_autopilot.config.settings import get_settings

logger = logging.getLogger(__name__)


class MetabaseClient:
    def __init__(self) -> None:
        self.settings = get_settings()
        self._dashboards_by_key: dict[str, dict[str, Any]] = {}
        self._base_url = self.settings.metabase_url.rstrip("/")
        self._client = httpx.Client(timeout=15, follow_redirects=True)

    def _headers(self) -> dict[str, str]:
        return {"X-API-Key": self.settings.metabase_api_key}

    def test_connection(self) -> dict:
        if self.settings.metabase_mock_mode:
            return {"ok": True, "mode": "mock"}

        resp = self._client.get(
            f"{self._base_url}/api/user/current", headers=self._headers()
        )
        resp.raise_for_status()
        body = resp.json()
        return {"ok": bool(body.get("id")), "mode": "live"}

    def create_card(self, name: str, sql: str) -> str:
        if self.settings.metabase_mock_mode:
            return f"card_{uuid4().hex[:10]}"

        r = self._client.post(
            f"{self._base_url}/api/card",
            headers=self._headers(),
            json={
                "name": name,
                "dataset_query": {"type": "native", "native": {"query": sql}},
                "display": "line",
            },
        )
        r.raise_for_status()
        return str(r.json()["id"])

    def create_or_update_dashboard(
        self, key: str, card_ids: list[str], layout: list[dict[str, int]], name: str
    ) -> str:
        if self.settings.metabase_mock_mode:
            existing = self._dashboards_by_key.get(key)
            if existing is not None:
                existing["card_ids"] = card_ids
                existing["layout"] = layout
                existing["name"] = name
                return existing["id"]
            dash_id = f"dash_{uuid4().hex[:10]}"
            self._dashboards_by_key[key] = {
                "id": dash_id, "card_ids": card_ids, "layout": layout, "name": name,
            }
            return dash_id

        existing = self._dashboards_by_key.get(key)
        if existing is None:
            r = self._client.post(
                f"{self._base_url}/api/dashboard",
                headers=self._headers(),
                json={"name": name},
            )
            r.raise_for_status()
            dash_id = str(r.json()["id"])
        else:
            dash_id = str(existing["id"])
        cards_payload = []
        for i, card_id in enumerate(card_ids):
            pos = layout[i]
            cards_payload.append(
                {
                    "card_id": int(card_id),
                    "row": pos["row"],
                    "col": pos["col"],
                    "size_x": pos["size_x"],
                    "size_y": pos["size_y"],
                }
            )
        rc = self._client.put(
            f"{self._base_url}/api/dashboard/{dash_id}/cards",
            headers=self._headers(),
            json={"cards": cards_payload},
        )
        rc.raise_for_status()
        self._dashboards_by_key[key] = {
            "id": dash_id, "card_ids": card_ids, "layout": layout, "name": name,
        }
        return dash_id
