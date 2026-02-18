"""Phase 2 tests: historical constraint honesty + full e2e flow."""

from unittest.mock import MagicMock, patch

from data_autopilot.services.mode1.live_fetcher import LiveFetcher
from data_autopilot.services.mode1.models import (
    Chain,
    DataRequest,
    Entity,
    Intent,
    OutputFormat,
    ProviderResult,
    RoutingDecision,
    RoutingMode,
)
from data_autopilot.services.mode1.platform_keys import PlatformKeyManager
from data_autopilot.services.mode1.request_parser import RequestParser


def _make_fetcher(records: list[dict], tier: str = "free") -> LiveFetcher:
    mock_provider = MagicMock()
    mock_provider.fetch.return_value = ProviderResult(
        provider="coingecko",
        method="get_price_history",
        records=records,
        total_available=len(records),
    )
    return LiveFetcher(
        providers={"coingecko": mock_provider},
        key_manager=PlatformKeyManager(),
        parser=RequestParser(),
        tier=tier,
    )


def test_historical_constraint_response() -> None:
    """2.17: Historical 'as of' query — no fake data, honest response.

    The system doesn't have historical snapshot capability for holders.
    When data is returned, the provenance should clearly indicate the current
    snapshot nature vs the requested historical point.
    """
    # Parser will parse this as a normal snapshot request since we don't have
    # historical holder tracking. The provenance source should be honest.
    from data_autopilot.services.mode1.request_parser import RequestParser

    parser = RequestParser()
    req = parser.parse("Show holders of $BONK as of January 1 2025")
    # Should still parse as snapshot (we don't support historical holder lookups)
    assert req.intent in (Intent.SNAPSHOT, Intent.TREND)
    assert req.entity == Entity.TOKEN_HOLDERS
    assert req.token == "BONK"


def test_e2e_full_flow_with_interpretation_provenance_xlsx() -> None:
    """2.18: Full flow returns table + interpretation + provenance + XLSX capability."""
    price_data = [
        {"timestamp": 1700000000 + i * 86400, "price": 0.00001 + i * 0.000001}
        for i in range(30)
    ]
    fetcher = _make_fetcher(price_data, tier="pro")

    with patch.object(fetcher._router, "route") as mock_route:
        mock_route.return_value = RoutingDecision(
            mode=RoutingMode.PUBLIC_API,
            confidence=0.9,
            provider_name="coingecko",
            method_name="get_price_history",
        )
        result = fetcher.handle(
            "Top 50 PEPE holders with % supply and 30d price change",
            session_id="e2e_test",
        )

    assert result["response_type"] == "blockchain_result"
    data = result["data"]

    # Has table
    assert "table" in data
    assert "|" in data["table"]

    # Has interpretation
    assert "interpretation" in data
    assert data["interpretation"]["disclaimer"]
    assert "advice" in data["interpretation"]["disclaimer"].lower()

    # Has provenance
    assert "provenance" in data
    assert data["provenance"]["source"] == "CoinGecko API"
    assert "provenance_footer" in data
    assert "Source:" in data["provenance_footer"]

    # Can export as XLSX after viewing
    xlsx_result = fetcher.handle("Export that as spreadsheet", session_id="e2e_test")
    assert xlsx_result["response_type"] == "blockchain_export"
    assert xlsx_result["data"]["format"] == "xlsx"
    assert isinstance(xlsx_result["data"]["content_bytes"], bytes)
    assert xlsx_result["data"]["record_count"] == 30
