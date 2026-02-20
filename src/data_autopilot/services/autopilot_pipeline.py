from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any

from data_autopilot.config.settings import get_settings
from data_autopilot.services.bigquery_connector import BigQueryConnector
from data_autopilot.services.dashboard_designer import DashboardDesigner, DashboardCardSpec
from data_autopilot.services.dashboard_service import LayoutEngine, CardDef
from data_autopilot.services.metabase_client import MetabaseClient

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    success: bool
    steps_completed: list[str] = field(default_factory=list)
    table_info: dict[str, Any] | None = None
    card_specs: list[DashboardCardSpec] | None = None
    card_ids: list[str] | None = None
    dashboard_id: str | None = None
    dashboard_url: str | None = None
    error: str | None = None
    error_step: str | None = None


class AutopilotPipeline:
    def __init__(
        self,
        bq: BigQueryConnector | None = None,
        designer: DashboardDesigner | None = None,
        metabase: MetabaseClient | None = None,
        layout: LayoutEngine | None = None,
    ) -> None:
        self.bq = bq or BigQueryConnector()
        self.designer = designer or DashboardDesigner()
        self.metabase = metabase or MetabaseClient()
        self.layout = layout or LayoutEngine()
        self.settings = get_settings()

    @staticmethod
    def _sanitize_table_name(entity: str, token: str) -> str:
        raw = f"{entity}_{token}" if token else entity
        sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", raw).strip("_").lower()
        if not sanitized:
            sanitized = "autopilot_data"
        if sanitized[0].isdigit():
            sanitized = f"t_{sanitized}"
        return sanitized[:128]

    def run(
        self,
        prompt: str,
        records: list[dict[str, Any]],
        entity: str = "",
        token: str = "",
        tenant_id: str = "default",
    ) -> PipelineResult:
        result = PipelineResult(success=False)

        # Step 1: Ingest into BigQuery
        table_name = self._sanitize_table_name(entity, token)
        try:
            table_info = self.bq.create_table_from_records(
                records=records, table_name=table_name, dataset_id="autopilot"
            )
            result.table_info = table_info
            result.steps_completed.append("bigquery_ingest")
        except Exception as exc:
            logger.error("Autopilot: BigQuery ingest failed: %s", exc, exc_info=True)
            result.error = str(exc)
            result.error_step = "bigquery_ingest"
            return result

        # Build fully qualified table name
        if self.settings.bigquery_mock_mode:
            table_fq = f"autopilot.{table_name}"
        else:
            table_fq = table_info.get("table_fq", f"{self.settings.bigquery_project_id}.autopilot.{table_name}")

        columns = table_info.get("columns", self.bq._infer_schema(records))
        sample_rows = records[:5]

        # Step 2: LLM designs dashboard cards
        try:
            card_specs = self.designer.design(
                table_fq=table_fq,
                columns=columns,
                sample_rows=sample_rows,
                user_prompt=prompt,
            )
            result.card_specs = card_specs
            result.steps_completed.append("dashboard_design")
        except Exception as exc:
            logger.error("Autopilot: Dashboard design failed: %s", exc, exc_info=True)
            result.error = str(exc)
            result.error_step = "dashboard_design"
            return result

        if not card_specs:
            result.error = "No dashboard cards were designed"
            result.error_step = "dashboard_design"
            return result

        # Step 3: Create Metabase cards
        card_ids: list[str] = []
        card_defs: list[CardDef] = []
        try:
            for spec in card_specs:
                card_id = self.metabase.create_card(
                    name=spec.name,
                    sql=spec.sql,
                    display=spec.chart_type,
                )
                card_ids.append(card_id)
                card_defs.append(CardDef(name=spec.name, sql=spec.sql, width=spec.width, height=spec.height))
            result.card_ids = card_ids
            result.steps_completed.append("metabase_cards")
        except Exception as exc:
            logger.error("Autopilot: Metabase card creation failed: %s", exc, exc_info=True)
            result.card_ids = card_ids  # partial
            result.error = str(exc)
            result.error_step = "metabase_cards"
            return result

        # Step 4: Create dashboard
        try:
            layout = self.layout.arrange(card_defs)
            dash_key = f"{tenant_id}:autopilot:{table_name}"
            dash_name = f"Autopilot: {entity or table_name}"
            if token:
                dash_name += f" ({token})"

            dashboard_id = self.metabase.create_or_update_dashboard(
                key=dash_key,
                card_ids=card_ids,
                layout=layout,
                name=dash_name,
            )
            result.dashboard_id = dashboard_id
            base_url = self.settings.metabase_url.rstrip("/")
            result.dashboard_url = f"{base_url}/dashboard/{dashboard_id}"
            result.steps_completed.append("metabase_dashboard")
        except Exception as exc:
            logger.error("Autopilot: Dashboard creation failed: %s", exc, exc_info=True)
            result.error = str(exc)
            result.error_step = "metabase_dashboard"
            return result

        result.success = True
        return result
