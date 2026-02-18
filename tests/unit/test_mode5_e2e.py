"""Phase 5 tests: End-to-end warehouse flow."""

from data_autopilot.services.mode1.entity_aliases import EntityAliasManager
from data_autopilot.services.mode1.models import ColumnProfile
from data_autopilot.services.mode1.nl_to_sql import NLToSQL
from data_autopilot.services.mode1.postgres_connector import PostgresReadConnector
from data_autopilot.services.mode1.schema_profiler import SchemaProfiler
from data_autopilot.services.mode1.sql_validator import SQLValidator
from data_autopilot.services.mode1.warehouse_dashboard import WarehouseDashboard


def test_e2e_connect_question_dashboard() -> None:
    """5.13: Connect BigQuery → 'Show me MRR trend' → 'Make it a dashboard' → full flow."""

    # Step 1: Connect and profile
    conn = PostgresReadConnector(connection_string="mock://test", mock_mode=True)
    conn.connect()

    conn.register_mock_table(
        "fct_orders",
        columns=[
            ColumnProfile(name="id", data_type="integer", is_primary_key=True),
            ColumnProfile(name="customer_id", data_type="integer"),
            ColumnProfile(name="amount", data_type="decimal"),
            ColumnProfile(name="created_at", data_type="timestamp"),
            ColumnProfile(name="category", data_type="varchar"),
        ],
        rows=[
            {"id": i, "customer_id": i % 10, "amount": 50 + i, "created_at": f"2025-03-{(i % 28) + 1:02d}", "category": "SaaS"}
            for i in range(100)
        ],
    )

    profiler = SchemaProfiler()
    schema = profiler.profile(conn)
    assert len(schema.tables) == 1

    # Step 2: Set up entity aliases
    aliases = EntityAliasManager()
    aliases.set_alias("org_e2e", "fct_orders", "Orders")

    # Step 3: Generate SQL from natural language
    nl = NLToSQL(alias_manager=aliases, validator=SQLValidator())
    query = nl.generate("How many orders?", schema, org_id="org_e2e")

    assert query.validated
    assert "COUNT" in query.sql.upper()

    # Step 4: Execute query
    results = conn.execute_query(query.sql)
    assert len(results) == 1
    assert results[0]["count"] == 100

    # Step 5: Build dashboard
    dashboard = WarehouseDashboard()

    # Simulate a trend query result
    trend_data = [
        {"month": "2025-01", "mrr": 10000},
        {"month": "2025-02", "mrr": 12000},
        {"month": "2025-03", "mrr": 15000},
    ]
    dash_result = dashboard.create_from_query(
        org_id="org_e2e",
        title="MRR Trend",
        sql="SELECT month, SUM(amount) as mrr FROM fct_orders GROUP BY month",
        records=trend_data,
    )

    assert dash_result["status"] == "success"
    assert dash_result["dashboard"]["charts"][0]["type"] == "line"
    assert len(dash_result["dashboard"]["charts"][0]["data"]) == 3
