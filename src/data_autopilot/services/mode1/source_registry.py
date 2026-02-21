from __future__ import annotations

from data_autopilot.services.mode1.models import Chain, Entity

# Maps (chain, entity) → (provider_name, method_name)
SOURCE_REGISTRY: dict[tuple[Chain, Entity], tuple[str, str]] = {
    # Solana via Helius
    (Chain.SOLANA, Entity.TOKEN_HOLDERS): ("helius", "get_token_accounts"),
    (Chain.SOLANA, Entity.TOKEN_BALANCES): ("helius", "get_token_accounts"),
    (Chain.SOLANA, Entity.NFT_ASSET): ("helius", "get_asset"),
    (Chain.SOLANA, Entity.TRANSACTION_HISTORY): ("helius", "get_signatures"),
    # Ethereum via Alchemy
    (Chain.ETHEREUM, Entity.TOKEN_BALANCES): ("alchemy", "get_token_balances"),
    (Chain.ETHEREUM, Entity.ASSET_TRANSFERS): ("alchemy", "get_asset_transfers"),
    (Chain.ETHEREUM, Entity.LOGS): ("alchemy", "get_logs"),
    # Cross-chain token holders/balances default to Solana (Helius)
    (Chain.CROSS_CHAIN, Entity.TOKEN_HOLDERS): ("helius", "get_token_accounts"),
    (Chain.CROSS_CHAIN, Entity.TOKEN_BALANCES): ("helius", "get_token_accounts"),
    # Cross-chain via CoinGecko
    (Chain.CROSS_CHAIN, Entity.TOKEN_PRICE): ("coingecko", "get_price"),
    (Chain.CROSS_CHAIN, Entity.TOKEN_INFO): ("coingecko", "get_coin_info"),
    (Chain.CROSS_CHAIN, Entity.PRICE_HISTORY): ("coingecko", "get_price_history"),
    # Chain-specific price lookups also go to CoinGecko
    (Chain.SOLANA, Entity.TOKEN_PRICE): ("coingecko", "get_price"),
    (Chain.ETHEREUM, Entity.TOKEN_PRICE): ("coingecko", "get_price"),
    (Chain.SOLANA, Entity.PRICE_HISTORY): ("coingecko", "get_price_history"),
    (Chain.ETHEREUM, Entity.PRICE_HISTORY): ("coingecko", "get_price_history"),
    # DexScreener — DEX pair data
    (Chain.CROSS_CHAIN, Entity.DEX_PAIR): ("dexscreener", "search_pairs"),
    (Chain.SOLANA, Entity.DEX_PAIR): ("dexscreener", "get_pair"),
    (Chain.ETHEREUM, Entity.DEX_PAIR): ("dexscreener", "get_pair"),
    # DefiLlama — protocol + chain TVL
    (Chain.CROSS_CHAIN, Entity.PROTOCOL_TVL): ("defillama", "get_tvl"),
    (Chain.CROSS_CHAIN, Entity.CHAIN_TVL): ("defillama", "get_chain_tvl"),
    (Chain.SOLANA, Entity.PROTOCOL_TVL): ("defillama", "get_tvl"),
    (Chain.ETHEREUM, Entity.PROTOCOL_TVL): ("defillama", "get_tvl"),
}

# Fallback mappings: if primary provider fails, try fallback
FALLBACK_REGISTRY: dict[tuple[str, str], tuple[str, str]] = {
    ("coingecko", "get_price"): ("dexscreener", "get_price"),
}


def lookup(chain: Chain, entity: Entity) -> tuple[str, str] | None:
    return SOURCE_REGISTRY.get((chain, entity))


def lookup_fallback(provider: str, method: str) -> tuple[str, str] | None:
    return FALLBACK_REGISTRY.get((provider, method))
