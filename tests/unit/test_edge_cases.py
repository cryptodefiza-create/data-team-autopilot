"""Edge-case, error-path, and boundary-condition tests.

Every test exercises real code paths (no unittest.mock of code under test).
Providers' built-in ``mock_mode`` is real code, so it's used where appropriate.
"""

from __future__ import annotations

import pytest

from data_autopilot.services.mode1.assessment_builder import AssessmentBuilder
from data_autopilot.services.mode1.cex_connected import CEXConnected
from data_autopilot.services.mode1.cex_public import CEXPublicData
from data_autopilot.services.mode1.contract_version import ContractVersionManager
from data_autopilot.services.mode1.data_transformer import DataTransformer
from data_autopilot.services.mode1.defi_analytics import DeFiAnalytics
from data_autopilot.services.mode1.models import (
    Chain,
    ContractDefaults,
    DataRequest,
    Entity,
    EntityConfig,
    Intent,
    MartTable,
    MetricDefinition,
    OutputFormat,
    ProviderResult,
    RoutingMode,
    SemanticContract,
)
from data_autopilot.services.mode1.onchain_analytics import OnChainAnalytics
from data_autopilot.services.mode1.platform_keys import PlatformKeyManager
from data_autopilot.services.mode1.promotion_gate import PromotionGate
from data_autopilot.services.mode1.request_parser import RequestParser
from data_autopilot.services.mode1.request_router import RequestRouter
from data_autopilot.services.mode1.response_formatter import ResponseFormatter
from data_autopilot.services.mode1.semantic_contract import SemanticContractManager
from data_autopilot.services.mode1.transform_dag import TransformDAG
from data_autopilot.services.mode1.wallet_labeler import WalletLabeler
from data_autopilot.services.providers.binance import BinanceProvider
from data_autopilot.services.providers.coingecko import resolve_token_id
from data_autopilot.services.providers.defillama import DefiLlamaProvider
from data_autopilot.services.providers.dexscreener import DexScreenerProvider
from data_autopilot.services.providers.snapshot_org import SnapshotProvider


# ---------------------------------------------------------------------------
# 1. RequestParser
# ---------------------------------------------------------------------------


class TestRequestParserEdgeCases:
    """Edge cases for keyword-based request parsing."""

    def test_empty_message(self) -> None:
        parser = RequestParser()
        req = parser.parse("")
        assert isinstance(req, DataRequest)
        assert req.raw_message == ""
        assert req.entity == Entity.TOKEN_PRICE
        assert req.token == ""
        assert req.address == ""

    def test_no_recognizable_entity(self) -> None:
        parser = RequestParser()
        req = parser.parse("hello world this is random text")
        assert req.entity == Entity.TOKEN_PRICE  # default fallback

    def test_multiple_addresses_extracts_first(self) -> None:
        parser = RequestParser()
        addr1 = "0x" + "a1" * 20
        addr2 = "0x" + "b2" * 20
        req = parser.parse(f"Compare {addr1} and {addr2}")
        assert req.address == addr1

    def test_only_time_range_no_entity(self) -> None:
        parser = RequestParser()
        req = parser.parse("show me last 14 days")
        assert req.intent == Intent.TREND
        assert req.time_range_days == 14

    def test_token_symbol_at_end_of_sentence(self) -> None:
        parser = RequestParser()
        req = parser.parse("What is the price of $BONK")
        assert req.token == "BONK"


# ---------------------------------------------------------------------------
# 2. RequestRouter
# ---------------------------------------------------------------------------


class TestRequestRouterEdgeCases:
    """Private-signal penalties and specific routing lookups."""

    @pytest.mark.parametrize("signal", ["our", "internal", "company"])
    def test_private_signals_penalize(self, signal: str) -> None:
        router = RequestRouter()
        req = DataRequest(
            raw_message=f"Show {signal} token holders on Solana",
            entity=Entity.TOKEN_HOLDERS,
            chain=Chain.SOLANA,
        )
        decision = router.route(req)
        assert decision.confidence < 0.7
        assert decision.mode == RoutingMode.ASK_USER

    def test_ethereum_token_balances_routes_alchemy(self) -> None:
        router = RequestRouter()
        req = DataRequest(
            raw_message="Show token balances on Ethereum",
            entity=Entity.TOKEN_BALANCES,
            chain=Chain.ETHEREUM,
        )
        decision = router.route(req)
        assert decision.provider_name == "alchemy"
        assert decision.method_name == "get_token_balances"

    def test_cross_chain_price_routes_coingecko(self) -> None:
        router = RequestRouter()
        req = DataRequest(
            raw_message="Get price of SOL",
            entity=Entity.TOKEN_PRICE,
            chain=Chain.CROSS_CHAIN,
        )
        decision = router.route(req)
        assert decision.provider_name == "coingecko"
        assert decision.method_name == "get_price"


# ---------------------------------------------------------------------------
# 3. Provider dispatch & data
# ---------------------------------------------------------------------------


class TestProviderEdgeCases:
    """Unknown methods and mock-data shape validation."""

    @pytest.mark.parametrize(
        "provider_cls,kwargs",
        [
            (BinanceProvider, {"mock_mode": True}),
            (DexScreenerProvider, {"mock_mode": True}),
            (DefiLlamaProvider, {"mock_mode": True}),
            (SnapshotProvider, {"mock_mode": True}),
        ],
    )
    def test_unknown_method_returns_error(self, provider_cls: type, kwargs: dict) -> None:
        provider = provider_cls(**kwargs)
        result = provider.fetch("nonexistent_method", {})
        assert isinstance(result, ProviderResult)
        assert result.error is not None
        assert "Unknown method" in result.error

    def test_binance_depth_report_values(self) -> None:
        bp = BinanceProvider(mock_mode=True)
        report = bp.get_depth_report("SOLUSDT")
        assert report.best_bid < report.best_ask
        assert report.bid_ask_spread > 0
        assert report.bid_depth_1pct > 0
        assert report.ask_depth_1pct > 0

    def test_dexscreener_search_pairs_shape(self) -> None:
        ds = DexScreenerProvider(mock_mode=True)
        result = ds.fetch("search_pairs", {"query": "BONK"})
        assert result.succeeded
        rec = result.records[0]
        assert "base_token" in rec
        assert "quote_token" in rec
        assert "price_usd" in rec

    def test_defillama_tvl_positive(self) -> None:
        dl = DefiLlamaProvider(mock_mode=True)
        result = dl.fetch("get_tvl", {"protocol": "aave"})
        assert result.succeeded
        data = result.records[0]
        assert data["tvl"] > 0
        assert isinstance(data["currentChainTvls"], dict)

    def test_snapshot_proposals_shape(self) -> None:
        sp = SnapshotProvider(mock_mode=True)
        result = sp.fetch("get_proposals", {"space": "aave.eth"})
        assert result.succeeded
        p = result.records[0]
        assert "id" in p
        assert "title" in p
        assert "state" in p
        assert "votes" in p

    def test_coingecko_resolve_unknown_token(self) -> None:
        assert resolve_token_id("XYZUNKNOWN") == "xyzunknown"


# ---------------------------------------------------------------------------
# 4. OnChainAnalytics
# ---------------------------------------------------------------------------


class TestOnChainAnalyticsEdgeCases:
    """Empty inputs, zero supply, edge thresholds."""

    def test_whale_tracker_empty_holders(self) -> None:
        oca = OnChainAnalytics(mock_mode=True)
        oca.register_mock_holders("mintX", [])
        assert oca.whale_tracker("mintX") == []

    def test_whale_tracker_zero_total_supply(self) -> None:
        oca = OnChainAnalytics(mock_mode=True)
        oca.register_mock_holders("mintZ", [
            {"address": "w1", "balance": 0},
            {"address": "w2", "balance": 0},
        ])
        assert oca.whale_tracker("mintZ") == []

    def test_whale_tracker_all_small_holders(self) -> None:
        oca = OnChainAnalytics(mock_mode=True)
        holders = [{"address": f"w{i}", "balance": 1} for i in range(200)]
        oca.register_mock_holders("mintS", holders)
        # Each holds 0.5% — below 1% threshold
        whales = oca.whale_tracker("mintS", threshold_pct=1.0)
        assert len(whales) == 0

    def test_exchange_flow_no_transfers(self) -> None:
        oca = OnChainAnalytics(mock_mode=True)
        oca.register_mock_transfers("mintE", [])
        report = oca.exchange_flow("mintE")
        assert report.inflow_volume == 0
        assert report.outflow_volume == 0
        assert report.inflow_count == 0
        assert report.outflow_count == 0

    def test_wallet_overlap_one_token_empty(self) -> None:
        oca = OnChainAnalytics(mock_mode=True)
        oca.register_mock_holders("mintA", [{"address": "w1"}, {"address": "w2"}])
        oca.register_mock_holders("mintB", [])
        report = oca.wallet_overlap("mintA", "mintB")
        assert report.overlap_count == 0
        assert report.overlap_pct_a == 0.0


# ---------------------------------------------------------------------------
# 5. WalletLabeler
# ---------------------------------------------------------------------------


class TestWalletLabelerEdgeCases:
    """Address type detection, exchange check, custom override."""

    def test_solana_style_address_type(self) -> None:
        labeler = WalletLabeler()
        # 44-char base58 address (no 0x prefix)
        sol_addr = "7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj"
        result = labeler.enrich(sol_addr)
        assert result.type == "solana_account"

    def test_short_unknown_address_type(self) -> None:
        labeler = WalletLabeler()
        result = labeler.enrich("abc")
        assert result.type == ""

    def test_is_exchange_with_non_exchange(self) -> None:
        labeler = WalletLabeler()
        assert labeler.is_exchange("random_address_123") is False

    def test_custom_label_overrides_builtin(self) -> None:
        labeler = WalletLabeler()
        # Pick a known exchange address
        exchange_addr = "5tzFkiKscXHK5ZXCGbXZxdw7gTjjD1mBwuoFbhUvuAi9"
        labeler.add_custom_label("org1", exchange_addr, "Our Treasury", "treasury")
        result = labeler.enrich(exchange_addr, org_id="org1")
        assert result.label == "Our Treasury"
        assert result.source == "custom"


# ---------------------------------------------------------------------------
# 6. CEX (public + connected)
# ---------------------------------------------------------------------------


class TestCEXEdgeCases:
    """Unsupported exchange, depth values, empty portfolio, permissions, funding."""

    def test_unsupported_exchange_returns_empty_volume(self) -> None:
        cex = CEXPublicData()
        report = cex.trading_volume("SOLUSDT", exchange="kraken")
        assert report.exchange == "kraken"
        assert report.volume_24h == 0.0

    def test_order_book_depth_spread(self) -> None:
        cex = CEXPublicData()
        report = cex.order_book_depth("SOLUSDT")
        assert report.bid_ask_spread > 0
        assert report.best_bid < report.best_ask
        assert report.bid_depth_1pct > 0

    def test_empty_portfolio_balances(self) -> None:
        cex = CEXConnected(mock_mode=True)
        report = cex.portfolio_balance("org_empty")
        assert report.total_value_usd == 0
        assert report.assets == []

    def test_case_insensitive_permissions_rejected(self) -> None:
        cex = CEXConnected(mock_mode=True)
        result = cex.validate_key_permissions("binance", ["Trading", "read"])
        assert result["valid"] is False
        assert "trading" in result["unsafe_permissions"]

    def test_negative_funding_rate(self) -> None:
        bp = BinanceProvider(mock_mode=True)
        bp.register_mock_data("get_funding_rate", {
            "symbol": "BTCUSDT",
            "lastFundingRate": "-0.0005",
            "nextFundingTime": "1700000000000",
        })
        report = bp.get_funding_report("BTCUSDT")
        assert report.interpretation == "shorts paying longs"


# ---------------------------------------------------------------------------
# 7. DeFiAnalytics
# ---------------------------------------------------------------------------


class TestDeFiAnalyticsEdgeCases:
    """Governance, TVL breakdown, pool analytics, revenue trend."""

    def test_governance_activity_success(self) -> None:
        defi = DeFiAnalytics()
        result = defi.governance_activity("aave")
        assert "active_proposals" in result
        assert "total_proposals" in result
        assert "voter_participation_trend" in result

    def test_protocol_tvl_breakdown(self) -> None:
        defi = DeFiAnalytics()
        result = defi.protocol_tvl_breakdown("aave")
        assert result["protocol"] == "aave"
        assert result["tvl"] > 0
        assert isinstance(result["chains"], dict)

    def test_pool_analytics_with_custom_data(self) -> None:
        ds = DexScreenerProvider(mock_mode=True)
        ds.register_mock_pool("custom_pool", {
            "pairAddress": "custom_pool",
            "baseToken": {"symbol": "AAA", "name": "TokenA"},
            "quoteToken": {"symbol": "BBB", "name": "TokenB"},
            "liquidity": {"usd": 2_000_000},
            "volume": {"h24": 500_000},
            "priceUsd": "1.23",
            "dexId": "orca",
        })
        defi = DeFiAnalytics(dexscreener=ds)
        pool = defi.pool_analytics("custom_pool")
        assert pool.token_0 == "AAA"
        assert pool.token_1 == "BBB"
        assert pool.tvl == 2_000_000
        assert pool.volume_24h == 500_000

    def test_revenue_trend_increasing(self) -> None:
        dl = DefiLlamaProvider(mock_mode=True)
        # First-half fees much smaller than second-half → trend = increasing
        daily = [
            {"date": f"d{i}", "fees": 1_000 if i < 5 else 10_000, "revenue": 100}
            for i in range(10)
        ]
        dl.register_mock_data("get_fees", {
            "name": "test",
            "total_fees": sum(d["fees"] for d in daily),
            "protocol_revenue": sum(d["revenue"] for d in daily),
            "daily": daily,
        })
        report = dl.get_revenue_report("test")
        assert report.trend == "increasing"


# ---------------------------------------------------------------------------
# 8. ResponseFormatter
# ---------------------------------------------------------------------------


class TestResponseFormatterEdgeCases:
    """Error results, empty records, >50 rows, raw output."""

    def test_error_result_formatting(self) -> None:
        fmt = ResponseFormatter()
        result = ProviderResult(provider="test", method="m", error="timeout")
        out = fmt.format(result)
        assert out["response_type"] == "error"
        assert "provider_error" in out["warnings"]

    def test_empty_records_table(self) -> None:
        fmt = ResponseFormatter()
        result = ProviderResult(provider="test", method="m", records=[])
        out = fmt.format(result)
        assert "_No records_" in out["data"]["table"]

    def test_more_than_50_records_truncated_note(self) -> None:
        fmt = ResponseFormatter()
        records = [{"id": i, "value": i * 10} for i in range(60)]
        result = ProviderResult(
            provider="test", method="m", records=records, total_available=60,
        )
        out = fmt.format(result)
        table = out["data"]["table"]
        assert "more rows" in table

    def test_raw_output_no_table(self) -> None:
        fmt = ResponseFormatter()
        result = ProviderResult(
            provider="test", method="m", records=[{"x": 1}], total_available=1,
        )
        out = fmt.format(result, output=OutputFormat.RAW)
        assert "table" not in out["data"]
        assert out["data"]["records"] == [{"x": 1}]


# ---------------------------------------------------------------------------
# 9. DataTransformer
# ---------------------------------------------------------------------------


class TestDataTransformerEdgeCases:
    """Boundary filters, non-numeric values, sort order, aggregation."""

    def test_filter_min_exact_boundary_included(self) -> None:
        dt = DataTransformer()
        data = [{"v": 10}, {"v": 20}, {"v": 30}]
        out = dt.filter(data, {"v_min": 20})
        assert {"v": 20} in out
        assert len(out) == 2

    def test_filter_max_exact_boundary_included(self) -> None:
        dt = DataTransformer()
        data = [{"v": 10}, {"v": 20}, {"v": 30}]
        out = dt.filter(data, {"v_max": 20})
        assert {"v": 20} in out
        assert len(out) == 2

    def test_filter_non_numeric_field_excluded(self) -> None:
        dt = DataTransformer()
        data = [{"v": 10}, {"v": "abc"}, {"v": 30}]
        out = dt.filter(data, {"v_min": 5})
        assert len(out) == 2  # "abc" excluded

    def test_sort_descending_first_is_highest(self) -> None:
        dt = DataTransformer()
        data = [{"v": 3}, {"v": 1}, {"v": 2}]
        out = dt.sort(data, "v", descending=True)
        assert out[0]["v"] == 3

    def test_aggregate_group_by(self) -> None:
        dt = DataTransformer()
        data = [
            {"category": "A", "amount": 10},
            {"category": "A", "amount": 20},
            {"category": "B", "amount": 5},
        ]
        out = dt.aggregate(data, "category", ["amount"])
        groups = {r["category"]: r for r in out}
        assert groups["A"]["count"] == 2
        assert groups["A"]["amount_sum"] == 30
        assert groups["B"]["count"] == 1
        assert groups["B"]["amount_sum"] == 5


# ---------------------------------------------------------------------------
# 10. PromotionGate
# ---------------------------------------------------------------------------


class TestPromotionGateEdgeCases:
    """Empty mart, high null rate, negative metric, no source entities."""

    def _contract(self) -> SemanticContract:
        return SemanticContract(
            org_id="org1",
            entities=[EntityConfig(name="orders", primary_key="id")],
            metrics=[MetricDefinition(name="revenue", definition="SUM(amount)")],
        )

    def test_empty_mart_fails_row_count(self) -> None:
        gate = PromotionGate()
        mart = MartTable(name="empty_mart", row_count=0, records=[])
        result = gate.validate(mart, self._contract())
        assert result.passed is False
        failed_names = [c.name for c in result.failures]
        assert "row_count" in failed_names

    def test_high_null_rate_fails(self) -> None:
        gate = PromotionGate()
        records = [{"id": i, "val": None} for i in range(10)]
        mart = MartTable(name="null_mart", row_count=10, records=records)
        result = gate.validate(mart, self._contract())
        assert result.passed is False
        failed_names = [c.name for c in result.failures]
        assert "null_rate" in failed_names

    def test_negative_metric_fails(self) -> None:
        gate = PromotionGate()
        records = [{"id": 1, "_revenue": -500}]
        mart = MartTable(
            name="neg_mart", row_count=1, records=records, source_entities=["orders"],
        )
        result = gate.validate(mart, self._contract())
        assert result.passed is False
        failed_names = [c.name for c in result.failures]
        assert "metric_ranges" in failed_names

    def test_no_source_entities_passes(self) -> None:
        gate = PromotionGate()
        records = [{"id": 1, "val": 100}]
        mart = MartTable(
            name="ok_mart", row_count=1, records=records, source_entities=[],
        )
        result = gate.validate(mart, self._contract())
        assert result.passed is True


# ---------------------------------------------------------------------------
# 11. TransformDAG
# ---------------------------------------------------------------------------


class TestTransformDAGEdgeCases:
    """No runner, reset, diamond dependency."""

    def test_node_with_no_runner_skipped(self) -> None:
        dag = TransformDAG()
        dag.add_node("orphan", "staging")
        results = dag.execute({})
        assert results["orphan"]["status"] == "skipped"

    def test_reset_after_execution(self) -> None:
        dag = TransformDAG()
        dag.add_node("a", "raw")
        dag.execute({"a": lambda: "done"})
        node = dag.get_node("a")
        assert node is not None and node.status == "completed"
        dag.reset()
        assert node.status == "pending"

    def test_diamond_dependency_order(self) -> None:
        dag = TransformDAG()
        dag.add_node("A", "raw")
        dag.add_node("B", "staging", depends_on=["A"])
        dag.add_node("C", "staging", depends_on=["A"])
        dag.add_node("D", "marts", depends_on=["B", "C"])

        call_order: list[str] = []
        runners = {
            "A": lambda: call_order.append("A"),
            "B": lambda: call_order.append("B"),
            "C": lambda: call_order.append("C"),
            "D": lambda: call_order.append("D"),
        }
        results = dag.execute(runners)
        assert results["D"]["status"] == "completed"
        assert call_order.index("A") < call_order.index("B")
        assert call_order.index("A") < call_order.index("C")
        assert call_order.index("B") < call_order.index("D")
        assert call_order.index("C") < call_order.index("D")
        assert call_order.count("D") == 1


# ---------------------------------------------------------------------------
# 12. PlatformKeyManager
# ---------------------------------------------------------------------------


class TestPlatformKeyManagerEdgeCases:
    """Unregistered provider, empty keys, round-robin wrap."""

    def test_unregistered_provider_returns_none(self) -> None:
        km = PlatformKeyManager()
        assert km.acquire("nonexistent") is None

    def test_empty_key_list_returns_none(self) -> None:
        km = PlatformKeyManager()
        km.register("empty", [])
        assert km.acquire("empty") is None

    def test_round_robin_wraps(self) -> None:
        km = PlatformKeyManager()
        km.register("prov", ["k1", "k2", "k3"])
        results = [km.acquire("prov") for _ in range(6)]
        assert results == ["k1", "k2", "k3", "k1", "k2", "k3"]


# ---------------------------------------------------------------------------
# 13. ContractVersionManager
# ---------------------------------------------------------------------------


class TestContractVersionManagerEdgeCases:
    """Rollback at v1, compare nonexistent, sequential updates."""

    def _setup(self) -> tuple[ContractVersionManager, str]:
        scm = SemanticContractManager()
        scm.create(
            "org_cv",
            metrics=[MetricDefinition(name="rev", definition="SUM(amount)")],
            defaults=ContractDefaults(currency="USD"),
        )
        cvm = ContractVersionManager(scm)
        return cvm, "org_cv"

    def test_rollback_at_v1_raises(self) -> None:
        cvm, org = self._setup()
        with pytest.raises(ValueError, match="already at version 1"):
            cvm.rollback(org)

    def test_compare_nonexistent_version(self) -> None:
        cvm, org = self._setup()
        result = cvm.compare_versions(org, 1, 99)
        assert "error" in result

    def test_three_sequential_updates(self) -> None:
        cvm, org = self._setup()
        v2 = cvm.update(org, {"defaults": {"currency": "EUR"}})
        v3 = cvm.update(org, {"defaults": {"currency": "GBP"}})
        v4 = cvm.update(org, {"defaults": {"currency": "JPY"}})
        assert v2 == 2
        assert v3 == 3
        assert v4 == 4
        assert cvm.get_version(org, 2) is not None
        assert cvm.get_version(org, 3) is not None
        assert cvm.get_version(org, 4) is not None


# ---------------------------------------------------------------------------
# 14. AssessmentBuilder
# ---------------------------------------------------------------------------


class TestAssessmentBuilderEdgeCases:
    """Partial data availability: no analytics, no pools, empty panels."""

    def test_dao_with_no_analytics(self) -> None:
        defi = DeFiAnalytics()
        cex = CEXPublicData()
        builder = AssessmentBuilder(analytics=None, defi=defi, cex=cex)
        report = builder.build_dao_assessment(
            org_id="org1", mint="mintX", token_symbol="X",
        )
        panel_titles = [p.title for p in report.panels]
        # No analytics -> no holder/whale/flow panels
        assert "Top Holders & Whale Tracker" not in panel_titles
        # CEX panel should still be present
        assert "CEX Trading Volume" in panel_titles

    def test_defi_with_no_pools(self) -> None:
        defi = DeFiAnalytics()
        builder = AssessmentBuilder(defi=defi)
        report = builder.build_defi_assessment(
            org_id="org1", protocol_name="uniswap", pool_addresses=[],
        )
        panel_titles = [p.title for p in report.panels]
        assert "Top Pool Analytics" not in panel_titles
        # TVL and governance panels still present
        assert "Protocol TVL Breakdown" in panel_titles

    def test_empty_panels_memo_has_header(self) -> None:
        builder = AssessmentBuilder()  # no providers at all
        report = builder.build_dao_assessment(org_id="org1", mint="m")
        assert report.panels == []
        assert "Weekly Intelligence Memo" in report.memo
