import time

from data_autopilot.services.mode1.platform_keys import PlatformKeyManager
from data_autopilot.services.providers.coingecko import CoinGeckoProvider, resolve_token_id
from data_autopilot.services.providers.helius import HeliusProvider
from data_autopilot.services.mode1.models import ProviderResult


def test_helius_token_accounts_valid_structure() -> None:
    """1.8: Helius fetch returns valid ProviderResult (bad key → error, no crash)."""
    provider = HeliusProvider(api_key="invalid_key_for_test")
    result = provider.fetch("get_token_accounts", {"address": "11111111111111111111111111111111"})
    assert isinstance(result, ProviderResult)
    assert result.provider == "helius"
    assert result.method == "get_token_accounts"
    # With a bad key, we expect an error (no crash)
    assert result.error is not None


def test_helius_bad_key_no_crash() -> None:
    """1.9: Helius with bad key returns error in ProviderResult."""
    provider = HeliusProvider(api_key="bad_key")
    result = provider.fetch("get_asset", {"address": "test_asset_id"})
    assert isinstance(result, ProviderResult)
    assert not result.succeeded
    assert result.error is not None


def test_coingecko_price_history_structure() -> None:
    """1.10: CoinGecko price history returns ProviderResult (may error on network)."""
    provider = CoinGeckoProvider(base_url="https://api.coingecko.invalid")
    result = provider.fetch("get_price_history", {"token": "BTC", "days": 7})
    assert isinstance(result, ProviderResult)
    assert result.provider == "coingecko"
    assert result.method == "get_price_history"
    # Unreachable endpoint → error
    assert result.error is not None


def test_coingecko_token_resolve_pepe() -> None:
    """1.11: CoinGecko token resolve $PEPE → correct CoinGecko ID."""
    assert resolve_token_id("$PEPE") == "pepe"
    assert resolve_token_id("PEPE") == "pepe"
    assert resolve_token_id("$ETH") == "ethereum"
    assert resolve_token_id("BTC") == "bitcoin"


def test_record_limit_enforcement() -> None:
    """1.12: Record limit truncation sets truncated=True."""
    from data_autopilot.services.mode1.live_fetcher import LiveFetcher
    from data_autopilot.services.mode1.platform_keys import PlatformKeyManager
    from data_autopilot.services.mode1.request_parser import RequestParser
    from unittest.mock import MagicMock

    mock_provider = MagicMock()
    mock_provider.fetch.return_value = ProviderResult(
        provider="test",
        method="test_method",
        records=[{"id": i} for i in range(200)],
        total_available=200,
    )

    fetcher = LiveFetcher(
        providers={"test_provider": mock_provider},
        key_manager=PlatformKeyManager(),
        parser=RequestParser(),
        tier="free",  # limit = 100
    )
    assert fetcher.record_limit == 100

    # Directly test the truncation logic via execute
    from data_autopilot.services.mode1.models import (
        Chain, DataRequest, Entity, Intent, RoutingMode,
    )
    from unittest.mock import patch

    req = DataRequest(
        raw_message="test",
        intent=Intent.SNAPSHOT,
        chain=Chain.SOLANA,
        entity=Entity.TOKEN_HOLDERS,
        token="TEST",
    )

    with patch.object(fetcher._router, "route") as mock_route:
        from data_autopilot.services.mode1.models import RoutingDecision
        mock_route.return_value = RoutingDecision(
            mode=RoutingMode.PUBLIC_API,
            confidence=0.9,
            provider_name="test_provider",
            method_name="test_method",
        )
        result = fetcher.execute(req)
        assert result["data"]["truncated"] is True
        assert len(result["data"]["records"]) == 100


def test_key_rotation_after_rate_limit() -> None:
    """1.13: PlatformKeyManager switches keys after rate limit."""
    mgr = PlatformKeyManager()
    mgr.register("helius", ["key_a", "key_b"])

    first = mgr.acquire("helius")
    assert first == "key_a"

    mgr.mark_rate_limited("helius", "key_a")
    second = mgr.acquire("helius")
    assert second == "key_b"

    # key_a is still rate-limited
    mgr.mark_rate_limited("helius", "key_b")
    third = mgr.acquire("helius")
    assert third is None  # all keys exhausted
