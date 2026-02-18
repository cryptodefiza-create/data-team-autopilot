from __future__ import annotations

import logging
from typing import Any
from uuid import uuid4

from data_autopilot.services.mode1.models import SchemaProfile

logger = logging.getLogger(__name__)


class WarehouseDashboard:
    """Generates dashboard configurations from warehouse query results."""

    def __init__(self) -> None:
        self._dashboards: dict[str, dict[str, Any]] = {}

    def create_from_query(
        self,
        org_id: str,
        title: str,
        sql: str,
        records: list[dict[str, Any]],
        schema: SchemaProfile | None = None,
    ) -> dict[str, Any]:
        """Create a dashboard from query results."""
        if not records:
            return {"status": "error", "message": "No data to create dashboard from"}

        dashboard_id = f"dash_{uuid4().hex[:10]}"

        # Detect chart type from data shape
        chart_type = self._detect_chart_type(records)
        chart_config = self._build_chart(records, chart_type)

        dashboard = {
            "id": dashboard_id,
            "org_id": org_id,
            "title": title,
            "sql": sql,
            "charts": [chart_config],
            "record_count": len(records),
            "columns": list(records[0].keys()) if records else [],
        }

        self._dashboards[dashboard_id] = dashboard
        logger.info("Created warehouse dashboard %s for org %s", dashboard_id, org_id)

        return {
            "status": "success",
            "dashboard": dashboard,
        }

    def get_dashboard(self, dashboard_id: str) -> dict[str, Any] | None:
        return self._dashboards.get(dashboard_id)

    def _detect_chart_type(self, records: list[dict[str, Any]]) -> str:
        """Detect best chart type from data shape."""
        if not records:
            return "table"

        keys = list(records[0].keys())

        # If there's a time-like column and a numeric column → line chart
        time_cols = [k for k in keys if any(t in k.lower() for t in ("date", "month", "week", "day", "time", "period"))]
        numeric_cols = [k for k in keys if isinstance(records[0].get(k), (int, float))]

        if time_cols and numeric_cols:
            return "line"

        # If there's a category and a numeric → bar chart
        string_cols = [k for k in keys if isinstance(records[0].get(k), str)]
        if string_cols and numeric_cols:
            return "bar"

        # If just counts → metric
        if len(keys) == 1 and isinstance(records[0].get(keys[0]), (int, float)):
            return "metric"

        return "table"

    def _build_chart(
        self, records: list[dict[str, Any]], chart_type: str
    ) -> dict[str, Any]:
        """Build chart configuration."""
        if not records:
            return {"type": "table", "data": []}

        keys = list(records[0].keys())

        if chart_type == "line":
            x_col = next(
                (k for k in keys if any(t in k.lower() for t in ("date", "month", "week", "day", "time"))),
                keys[0],
            )
            y_col = next(
                (k for k in keys if isinstance(records[0].get(k), (int, float)) and k != x_col),
                keys[-1],
            )
            return {
                "type": "line",
                "x": x_col,
                "y": y_col,
                "data": [{x_col: r.get(x_col), y_col: r.get(y_col)} for r in records],
            }

        if chart_type == "bar":
            label_col = next(
                (k for k in keys if isinstance(records[0].get(k), str)),
                keys[0],
            )
            value_col = next(
                (k for k in keys if isinstance(records[0].get(k), (int, float))),
                keys[-1],
            )
            return {
                "type": "bar",
                "x": label_col,
                "y": value_col,
                "data": [{label_col: r.get(label_col), value_col: r.get(value_col)} for r in records],
            }

        if chart_type == "metric":
            key = keys[0]
            return {
                "type": "metric",
                "label": key,
                "value": records[0][key],
            }

        return {
            "type": "table",
            "columns": keys,
            "data": records,
        }
