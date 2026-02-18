from data_autopilot.services.mode1.models import Chain, Entity, Intent
from data_autopilot.services.mode1.request_parser import RequestParser


parser = RequestParser()


def test_parse_holders_on_solana() -> None:
    """1.1: Parse holder request with token and chain."""
    req = parser.parse("Show me all holders of $BONK on Solana")
    assert req.intent == Intent.SNAPSHOT
    assert req.entity == Entity.TOKEN_HOLDERS
    assert req.token == "BONK"
    assert req.chain == Chain.SOLANA


def test_parse_price_trend_with_time_range() -> None:
    """1.2: Parse price trend with time range."""
    req = parser.parse("What's the price of ETH over the last 30 days?")
    assert req.intent == Intent.TREND
    assert req.entity == Entity.PRICE_HISTORY
    assert req.time_range_days == 30


def test_detect_chain_from_base58_address() -> None:
    """1.3: Detect Solana from base58 address."""
    req = parser.parse("Show balances for DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263")
    assert req.chain == Chain.SOLANA


def test_detect_chain_from_0x_address() -> None:
    """1.4: Detect Ethereum from 0x address."""
    req = parser.parse("Show transfers for 0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045")
    assert req.chain == Chain.ETHEREUM
    assert req.address == "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
