from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

from data_autopilot.services.llm_client import LLMClient

logger = logging.getLogger(__name__)


@dataclass
class DashboardCardSpec:
    name: str
    sql: str
    chart_type: str  # line, bar, number, table, pie
    width: str  # "full" or "half"
    height: int  # Metabase grid units


class DashboardDesigner:
    VALID_CHART_TYPES = {"line", "bar", "number", "table", "pie"}

    def __init__(self, llm: LLMClient | None = None) -> None:
        self.llm = llm or LLMClient()

    def design(
        self,
        table_fq: str,
        columns: list[dict[str, str]],
        sample_rows: list[dict[str, Any]],
        user_prompt: str,
    ) -> list[DashboardCardSpec]:
        if not self.llm.is_configured():
            return self._fallback_design(table_fq, columns)

        system_prompt = (
            "You are a dashboard designer. Given a BigQuery table schema, sample data, "
            "and a user's intent, design 3-5 dashboard cards.\n\n"
            "Return JSON with a single key 'cards' containing an array. Each card object has:\n"
            "- name: card title (string)\n"
            "- sql: a BigQuery SQL query against the provided table (string)\n"
            "- chart_type: one of line, bar, number, table, pie (string)\n"
            "- width: 'full' or 'half' (string)\n"
            "- height: Metabase grid height 4-10 (integer)\n\n"
            "Rules:\n"
            "- Use the fully qualified table name provided\n"
            "- number charts should return a single scalar\n"
            "- line/bar charts need a dimension column and a measure column\n"
            "- Always include at least one summary number card\n"
            "- Return ONLY valid JSON, no markdown"
        )

        col_desc = ", ".join(f"{c['name']} ({c['type']})" for c in columns)
        sample_text = json.dumps(sample_rows[:5], default=str)

        user_msg = (
            f"Table: {table_fq}\n"
            f"Columns: {col_desc}\n"
            f"Sample rows (up to 5): {sample_text}\n\n"
            f"User request: {user_prompt}\n\n"
            f"Design the dashboard cards."
        )

        try:
            result = self.llm.generate_json_with_meta(
                system_prompt=system_prompt, user_prompt=user_msg
            )
            if not result.succeeded:
                logger.warning("LLM dashboard design failed: %s", result.error)
                return self._fallback_design(table_fq, columns)

            cards_raw = result.content.get("cards", [])
            if not isinstance(cards_raw, list) or not cards_raw:
                logger.warning("LLM returned no cards, using fallback")
                return self._fallback_design(table_fq, columns)

            return self._parse_cards(cards_raw)
        except Exception as exc:
            logger.error("Dashboard design LLM call failed: %s", exc, exc_info=True)
            return self._fallback_design(table_fq, columns)

    def _parse_cards(self, cards_raw: list[dict[str, Any]]) -> list[DashboardCardSpec]:
        cards: list[DashboardCardSpec] = []
        for raw in cards_raw:
            chart_type = str(raw.get("chart_type", "table")).lower()
            if chart_type not in self.VALID_CHART_TYPES:
                chart_type = "table"
            width = str(raw.get("width", "full")).lower()
            if width not in {"full", "half"}:
                width = "full"
            height = raw.get("height", 6)
            if not isinstance(height, int) or height < 4:
                height = 6
            if height > 10:
                height = 10
            cards.append(
                DashboardCardSpec(
                    name=str(raw.get("name", "Untitled")),
                    sql=str(raw.get("sql", "")),
                    chart_type=chart_type,
                    width=width,
                    height=height,
                )
            )
        return cards

    @staticmethod
    def _fallback_design(
        table_fq: str, columns: list[dict[str, str]]
    ) -> list[DashboardCardSpec]:
        col_names = [c["name"] for c in columns]
        cards: list[DashboardCardSpec] = [
            DashboardCardSpec(
                name="Record Count",
                sql=f"SELECT COUNT(*) AS total_records FROM `{table_fq}`",
                chart_type="number",
                width="half",
                height=4,
            ),
            DashboardCardSpec(
                name="All Data",
                sql=f"SELECT * FROM `{table_fq}` LIMIT 100",
                chart_type="table",
                width="full",
                height=8,
            ),
        ]

        numeric_cols = [c["name"] for c in columns if c["type"] in ("INTEGER", "FLOAT")]
        if numeric_cols:
            agg_col = numeric_cols[0]
            cards.insert(
                1,
                DashboardCardSpec(
                    name=f"Avg {agg_col}",
                    sql=f"SELECT AVG({agg_col}) AS avg_{agg_col} FROM `{table_fq}`",
                    chart_type="number",
                    width="half",
                    height=4,
                ),
            )

        return cards
