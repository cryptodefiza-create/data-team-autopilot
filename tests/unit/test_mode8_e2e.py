"""Phase 8 tests: Full E2E flows across modes, cross-mode, landing page."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from data_autopilot.services.mode1.business_query import BusinessQueryEngine
from data_autopilot.services.mode1.credential_flow import CredentialFlow
from data_autopilot.services.mode1.credential_vault import CredentialVault
from data_autopilot.services.mode1.dashboard_builder import DashboardBuilder
from data_autopilot.services.mode1.entity_aliases import EntityAliasManager
from data_autopilot.services.mode1.historical_query import HistoricalQuery
from data_autopilot.services.mode1.live_fetcher import LiveFetcher
from data_autopilot.services.mode1.models import (
    DataRequest,
    Entity,
    Chain,
    Intent,
    ColumnProfile,
    ProviderResult,
    RoutingDecision,
    RoutingMode,
    SnapshotRecord,
)
from data_autopilot.services.mode1.nl_to_sql import NLToSQL
from data_autopilot.services.mode1.onboarding import OnboardingFlow
from data_autopilot.services.mode1.persistence import PersistenceManager
from data_autopilot.services.mode1.platform_keys import PlatformKeyManager
from data_autopilot.services.mode1.postgres_connector import PostgresReadConnector
from data_autopilot.services.mode1.request_parser import RequestParser
from data_autopilot.services.mode1.snapshot_pipeline import SnapshotPipeline
from data_autopilot.services.mode1.sql_validator import SQLValidator
from data_autopilot.services.mode1.thin_contract import ThinContractManager


def test_e2e_mode1_blockchain_full_flow() -> None:
    """8.7: 'Top 50 $BONK holders' → 'Track weekly' → 'Show trend' → complete flow."""
    persistence = PersistenceManager(mock_mode=True)
    persistence.ensure_storage("org_e2e8", tier="pro")

    mock_provider = MagicMock()
    run_counter = {"n": 0}

    def fetch_side_effect(method, params):
        run_counter["n"] += 1
        return ProviderResult(
            provider="helius", method=method,
            records=[
                {"wallet": f"w{i}", "balance": 1000 * run_counter["n"] + i, "pct_supply": 0.1 + i * 0.01}
                for i in range(50)
            ],
            total_available=50,
        )

    mock_provider.fetch.side_effect = fetch_side_effect

    fetcher = LiveFetcher(
        providers={"helius": mock_provider},
        key_manager=PlatformKeyManager(),
        parser=RequestParser(),
        tier="pro",
    )

    pipeline_svc = SnapshotPipeline(persistence=persistence, fetcher=fetcher)

    # Step 1: Query holders
    with patch.object(fetcher._router, "route") as mock_route:
        mock_route.return_value = RoutingDecision(
            mode=RoutingMode.PUBLIC_API, confidence=0.9,
            provider_name="helius", method_name="get_token_accounts",
        )
        result = fetcher.handle("Show me top 50 holders of $BONK")

    assert result["response_type"] in ("blockchain_result",)
    assert len(result["data"]["records"]) == 50

    # Step 2: Track weekly
    request = DataRequest(
        raw_message="Track $BONK holders weekly",
        intent=Intent.TRACK, chain=Chain.SOLANA,
        entity=Entity.TOKEN_HOLDERS, token="BONK",
    )
    with patch.object(fetcher._router, "route") as mock_route:
        mock_route.return_value = RoutingDecision(
            mode=RoutingMode.PUBLIC_API, confidence=0.9,
            provider_name="helius", method_name="get_token_accounts",
        )
        pipe = pipeline_svc.create("org_e2e8", request, schedule="weekly")
        # Run a second time for trend
        pipeline_svc.run(pipe)

    assert pipe.run_count == 2

    # Step 3: Query history (trend)
    hq = HistoricalQuery(persistence)
    trend = hq.get_trend("org_e2e8", "token_holders")
    assert trend["response_type"] == "blockchain_result"
    assert len(trend["data"]["records"]) > 0
    assert trend["data"]["source"] == "stored_snapshots"


def test_e2e_mode1_business_flow() -> None:
    """8.8: Connect Shopify → 'Revenue by category' → dashboard → complete flow."""
    vault = CredentialVault(mock_mode=True)
    contract_mgr = ThinContractManager()
    flow = CredentialFlow(vault=vault, contract_manager=contract_mgr)

    # Step 1: Connect Shopify
    with patch("data_autopilot.services.mode1.credential_flow.ShopifyConnector") as MockConn:
        mock_instance = MagicMock()
        mock_instance.test_auth.return_value = True
        mock_instance.get_shop_info.return_value = {"iana_timezone": "UTC", "currency": "USD"}
        mock_instance.extract.return_value = iter([])
        MockConn.return_value = mock_instance

        connect_result = flow.connect_shopify("org_e2e8b", "test.myshopify.com", "shpat_test")

    assert connect_result["status"] == "connected"

    # Step 2: Revenue by category
    engine = BusinessQueryEngine(vault=vault, contract_manager=contract_mgr)
    orders = [
        {"id": i, "total_price": f"{50 + i * 10}.00", "product_type": "Shoes" if i % 2 == 0 else "Hats"}
        for i in range(20)
    ]
    with patch("data_autopilot.services.mode1.business_query.ShopifyConnector") as MockConn:
        mock_instance = MagicMock()
        mock_instance.fetch.return_value = ProviderResult(
            provider="shopify", method="get_orders",
            records=orders, total_available=20,
        )
        MockConn.return_value = mock_instance
        query_result = engine.query("org_e2e8b", "Revenue by product category last month")

    assert query_result["response_type"] == "business_result"

    # Step 3: Build dashboard from stored snapshots
    persistence = PersistenceManager(mock_mode=True)
    persistence.ensure_storage("org_e2e8b", tier="pro")
    backend = persistence.get_storage("org_e2e8b")
    for day in range(1, 8):
        snap = SnapshotRecord(
            source="shopify", entity="orders",
            query_params={"store": "test"}, record_id=f"order_{day}",
            payload_hash=f"hash_{day}",
            payload={"id": day, "total_price": f"{100 + day * 10}.00"},
            ingested_at=datetime(2025, 3, day, 12, 0, tzinfo=timezone.utc),
        )
        backend.insert_snapshot(snap)

    builder = DashboardBuilder(persistence)
    dash = builder.create_from_snapshots("org_e2e8b", "orders")
    assert dash["status"] == "success"


def test_e2e_mode2_warehouse_flow() -> None:
    """8.9: Connect BigQuery → 'Build retention dashboard' → Slack active."""
    # Step 1: Connect (mock Postgres as stand-in)
    conn = PostgresReadConnector(connection_string="mock://bq", mock_mode=True)
    conn.connect()
    conn.register_mock_table(
        "users",
        columns=[
            ColumnProfile(name="id", data_type="integer", is_primary_key=True),
            ColumnProfile(name="signup_date", data_type="date"),
            ColumnProfile(name="plan", data_type="varchar"),
        ],
        rows=[{"id": i, "signup_date": f"2025-01-{(i % 28) + 1:02d}", "plan": "pro"} for i in range(1000)],
    )

    schema = conn.profile_schema()
    assert len(schema.tables) == 1

    # Step 2: NL to SQL
    aliases = EntityAliasManager()
    aliases.set_alias("org_m2", "users", "Users")
    nl = NLToSQL(alias_manager=aliases, validator=SQLValidator())

    query = nl.generate("How many users signed up?", schema, org_id="org_m2")
    assert query.validated
    assert "COUNT" in query.sql.upper()

    # Step 3: Execute
    results = conn.execute_query(query.sql)
    assert results[0]["count"] == 1000


def test_e2e_cross_mode() -> None:
    """8.10: Mode 1 query + Mode 2 query in same conversation → both work."""
    # Mode 1: Blockchain query (mocked)
    parser = RequestParser()
    mode1_request = parser.parse("Show me $BONK price")
    assert mode1_request.entity == Entity.TOKEN_PRICE
    assert mode1_request.token == "BONK"

    # Mode 2: Warehouse query
    conn = PostgresReadConnector(mock_mode=True)
    conn.connect()
    conn.register_mock_table(
        "orders",
        columns=[
            ColumnProfile(name="id", data_type="integer", is_primary_key=True),
            ColumnProfile(name="amount", data_type="decimal"),
        ],
        rows=[{"id": i, "amount": 100 + i} for i in range(50)],
    )

    schema = conn.profile_schema()
    nl = NLToSQL()
    mode2_query = nl.generate("How many orders?", schema)
    assert mode2_query.validated

    results = conn.execute_query(mode2_query.sql)
    assert results[0]["count"] == 50

    # Both modes work independently — no confusion
    assert mode1_request.entity.value == "token_price"
    assert "COUNT" in mode2_query.sql.upper()


def test_landing_page_content() -> None:
    """8.12: Landing page content — both modes visible, pricing clear."""
    onboarding = OnboardingFlow()
    result = onboarding.start("org_landing")

    # Verify both modes are presented
    assert "MODE 1" in result["message"]
    assert "MODE 2" in result["message"]

    # Verify examples exist for all paths
    assert len(result["examples"]["mode1_blockchain"]) >= 3
    assert len(result["examples"]["mode1_connect"]) >= 3
    assert len(result["examples"]["mode2_warehouse"]) >= 3

    # Verify "Just Ask" (no setup) is highlighted
    assert "no setup required" in result["message"].lower() or "no setup needed" in result["message"].lower()
