"""Phase 2 tests: DexScreener, DefiLlama providers, and fallback logic."""

from unittest.mock import MagicMock

from data_autopilot.services.mode1.models import (
    Chain,
    DataRequest,
    Entity,
    Intent,
    ProviderResult,
    RoutingMode,
)
from data_autopilot.services.mode1.live_fetcher import LiveFetcher
from data_autopilot.services.mode1.platform_keys import PlatformKeyManager
from data_autopilot.services.mode1.request_parser import RequestParser
from data_autopilot.services.providers.dexscreener import DexScreenerProvider
from data_autopilot.services.providers.defillama import DefiLlamaProvider


def test_dexscreener_pair_data() -> None:
    """2.6: DexScreener returns ProviderResult for pair search (error on bad endpoint)."""
    provider = DexScreenerProvider(base_url="https://api.dexscreener.invalid")
    result = provider.fetch("search_pairs", {"query": "PEPE"})
    assert isinstance(result, ProviderResult)
    assert result.provider == "dexscreener"
    assert result.error is not None  # unreachable endpoint


def test_defillama_tvl() -> None:
    """2.7: DefiLlama returns ProviderResult for TVL (error on bad endpoint)."""
    provider = DefiLlamaProvider(base_url="https://api.llama.invalid")
    result = provider.fetch("get_tvl", {"protocol": "aave"})
    assert isinstance(result, ProviderResult)
    assert result.provider == "defillama"
    assert result.error is not None  # unreachable endpoint


def test_provider_fallback() -> None:
    """2.8: When CoinGecko fails, falls back to DexScreener for price."""
    # Mock CoinGecko that fails
    mock_coingecko = MagicMock()
    mock_coingecko.fetch.return_value = ProviderResult(
        provider="coingecko", method="get_price", error="Rate limited"
    )

    # Mock DexScreener that succeeds
    mock_dexscreener = MagicMock()
    mock_dexscreener.fetch.return_value = ProviderResult(
        provider="dexscreener",
        method="get_price",
        records=[{"pair": "PEPE/WETH", "price_usd": "0.00001"}],
        total_available=1,
    )

    fetcher = LiveFetcher(
        providers={"coingecko": mock_coingecko, "dexscreener": mock_dexscreener},
        key_manager=PlatformKeyManager(),
        parser=RequestParser(),
        tier="free",
    )

    request = DataRequest(
        raw_message="Price of PEPE",
        intent=Intent.SNAPSHOT,
        chain=Chain.CROSS_CHAIN,
        entity=Entity.TOKEN_PRICE,
        token="PEPE",
    )

    from unittest.mock import patch
    from data_autopilot.services.mode1.models import RoutingDecision

    with patch.object(fetcher._router, "route") as mock_route:
        mock_route.return_value = RoutingDecision(
            mode=RoutingMode.PUBLIC_API,
            confidence=0.9,
            provider_name="coingecko",
            method_name="get_price",
        )
        result = fetcher.execute(request)

    # Should have succeeded via fallback
    assert result["response_type"] == "blockchain_result"
    assert result["data"]["records"][0]["pair"] == "PEPE/WETH"
