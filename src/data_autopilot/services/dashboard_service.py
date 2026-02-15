from __future__ import annotations

from dataclasses import dataclass
from sqlalchemy import select
from sqlalchemy.orm import Session

from data_autopilot.models.entities import ArtifactType, CatalogTable
from data_autopilot.services.artifact_service import ArtifactService
from data_autopilot.services.bigquery_connector import BigQueryConnector
from data_autopilot.services.connection_context import load_active_connection_credentials
from data_autopilot.services.cost_limiter import SlidingWindowCostLimiter
from data_autopilot.services.metabase_client import MetabaseClient
from data_autopilot.services.sql_safety import SqlSafetyEngine


@dataclass
class CardDef:
    name: str
    sql: str
    width: str
    height: int


class LayoutEngine:
    def arrange(self, cards: list[CardDef]) -> list[dict[str, int]]:
        positions: list[dict[str, int]] = []
        row = 0
        left_half_height: int | None = None
        for card in cards:
            if card.width == "full":
                if left_half_height is not None:
                    row += left_half_height
                    left_half_height = None
                positions.append({"col": 0, "row": row, "size_x": 18, "size_y": card.height})
                row += card.height
                continue

            if left_half_height is None:
                positions.append({"col": 0, "row": row, "size_x": 9, "size_y": card.height})
                left_half_height = card.height
            else:
                positions.append({"col": 9, "row": row, "size_x": 9, "size_y": card.height})
                row += max(left_half_height, card.height)
                left_half_height = None
        return positions


class DashboardService:
    def __init__(self) -> None:
        self.safety = SqlSafetyEngine()
        self.connector = BigQueryConnector()
        self.cost = SlidingWindowCostLimiter()
        self.metabase = MetabaseClient()
        self.layout = LayoutEngine()
        self.artifacts = ArtifactService()

    def _template_cards(self, include_revenue: bool) -> list[CardDef]:
        cards = [
            CardDef(
                name="DAU Trend",
                sql=(
                    "SELECT DATE(created_at) AS day, COUNT(DISTINCT user_id) AS dau "
                    "FROM analytics.events WHERE created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY) GROUP BY 1"
                ),
                width="full",
                height=8,
            ),
            CardDef(name="WAU", sql="SELECT COUNT(DISTINCT user_id) AS wau FROM analytics.events", width="half", height=4),
            CardDef(name="MAU", sql="SELECT COUNT(DISTINCT user_id) AS mau FROM analytics.events", width="half", height=4),
        ]
        if include_revenue:
            cards.append(
                CardDef(
                    name="Daily Revenue",
                    sql=(
                        "SELECT DATE(created_at) AS day, SUM(amount) AS revenue "
                        "FROM analytics.orders WHERE created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY) GROUP BY 1"
                    ),
                    width="full",
                    height=8,
                )
            )
        return cards

    def generate(self, db: Session, tenant_id: str) -> dict:
        _connection_id, creds = load_active_connection_credentials(db, tenant_id=tenant_id)
        if not self.connector.settings.bigquery_mock_mode and creds is None:
            raise ValueError("No active BigQuery connection for tenant")

        stmt = select(CatalogTable).where(CatalogTable.tenant_id == tenant_id, CatalogTable.table_name == "orders")
        include_revenue = db.execute(stmt).scalar_one_or_none() is not None
        cards = self._template_cards(include_revenue)

        rendered_cards: list[CardDef] = []
        card_ids: list[str] = []
        query_hashes: list[str] = []
        for card in cards:
            safety = self.safety.evaluate(card.sql)
            if not safety.allowed:
                raise ValueError(f"Blocked query in dashboard generation: {safety.reasons}")
            sql = safety.rewritten_sql or card.sql
            dry = self.connector.dry_run(sql, service_account_json=creds)
            budget = self.cost.check(tenant_id, dry.total_bytes_processed)
            if not budget.allowed:
                raise ValueError("Budget exceeded while generating dashboard")

            result = self.connector.execute_query(sql, service_account_json=creds)
            self.cost.record(tenant_id, result.get("actual_bytes", dry.total_bytes_processed))
            if not result.get("rows"):
                continue

            card_id = self.metabase.create_card(card.name, sql)
            rendered_cards.append(card)
            card_ids.append(card_id)
            query_hashes.append(str(abs(hash(sql))))

        layout = self.layout.arrange(rendered_cards)

        key = f"{tenant_id}:exec_overview"
        dash_id = self.metabase.create_or_update_dashboard(key=key, card_ids=card_ids, layout=layout, name="Exec Overview")

        artifact = self.artifacts.create_or_update(
            db,
            tenant_id=tenant_id,
            artifact_type=ArtifactType.DASHBOARD,
            data={"metabase_dashboard_id": dash_id, "layout": layout},
            query_hashes=query_hashes,
        )
        return {"artifact_id": artifact.id, "version": artifact.version, "metabase_dashboard_id": dash_id}
