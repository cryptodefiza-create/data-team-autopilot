from __future__ import annotations

import logging
import re

from data_autopilot.services.llm_client import LLMClient
from data_autopilot.services.mode1.models import Chain, DataRequest, Entity, Intent

logger = logging.getLogger(__name__)

_BASE58_PATTERN = re.compile(r"\b[1-9A-HJ-NP-Za-km-z]{32,44}\b")
_ETH_PATTERN = re.compile(r"\b0x[0-9a-fA-F]{40}\b")
_TOKEN_SYMBOL_PATTERN = re.compile(r"\$([A-Za-z]{2,10})")
_TIME_RANGE_PATTERN = re.compile(r"(\d+)\s*(?:d|day|days)")

_ENTITY_KEYWORDS: dict[str, Entity] = {
    "holder": Entity.TOKEN_HOLDERS,
    "holders": Entity.TOKEN_HOLDERS,
    "balance": Entity.TOKEN_BALANCES,
    "balances": Entity.TOKEN_BALANCES,
    "price": Entity.TOKEN_PRICE,
    "transfer": Entity.ASSET_TRANSFERS,
    "transfers": Entity.ASSET_TRANSFERS,
    "transaction": Entity.TRANSACTION_HISTORY,
    "transactions": Entity.TRANSACTION_HISTORY,
    "tx": Entity.TRANSACTION_HISTORY,
    "nft": Entity.NFT_ASSET,
    "log": Entity.LOGS,
    "logs": Entity.LOGS,
    "history": Entity.PRICE_HISTORY,
}

_CHAIN_KEYWORDS: dict[str, Chain] = {
    "solana": Chain.SOLANA,
    "sol": Chain.SOLANA,
    "ethereum": Chain.ETHEREUM,
    "eth": Chain.ETHEREUM,
    "erc20": Chain.ETHEREUM,
    "erc-20": Chain.ETHEREUM,
}


class RequestParser:
    def __init__(self, llm: LLMClient | None = None) -> None:
        self._llm = llm

    def parse(self, message: str) -> DataRequest:
        if self._llm and self._llm.is_configured():
            try:
                return self._llm_parse(message)
            except Exception as exc:
                logger.warning("LLM parse failed, falling back to keywords: %s", exc)
        return self._keyword_parse(message)

    def _keyword_parse(self, message: str) -> DataRequest:
        text = message.lower()
        chain = self._detect_chain(message)
        entity = Entity.TOKEN_PRICE
        intent = Intent.SNAPSHOT

        for keyword, ent in _ENTITY_KEYWORDS.items():
            if keyword in text:
                entity = ent
                break

        token = self._extract_token(message)
        address = self._extract_address(message)
        time_range = self._extract_time_range(message)

        if time_range > 0 or "history" in text or "trend" in text or "over" in text:
            intent = Intent.TREND
            if entity == Entity.TOKEN_PRICE:
                entity = Entity.PRICE_HISTORY

        return DataRequest(
            raw_message=message,
            intent=intent,
            chain=chain,
            entity=entity,
            token=token,
            address=address,
            time_range_days=time_range,
        )

    def _llm_parse(self, message: str) -> DataRequest:
        system_prompt = (
            "You parse blockchain data requests. Return JSON with keys: "
            "intent (snapshot|trend|lookup), chain (solana|ethereum|cross_chain), "
            "entity (token_holders|token_balances|token_price|token_info|price_history|"
            "asset_transfers|transaction_history|nft_asset|logs), "
            "token (symbol without $), address (if any), time_range_days (int, 0 if none)."
        )
        result = self._llm.generate_json(system_prompt=system_prompt, user_prompt=message)
        return DataRequest(
            raw_message=message,
            intent=Intent(result.get("intent", "snapshot")),
            chain=Chain(result.get("chain", "cross_chain")),
            entity=Entity(result.get("entity", "token_price")),
            token=str(result.get("token", "")),
            address=str(result.get("address", "")),
            time_range_days=int(result.get("time_range_days", 0)),
        )

    @staticmethod
    def _detect_chain(message: str) -> Chain:
        if _ETH_PATTERN.search(message):
            return Chain.ETHEREUM
        if _BASE58_PATTERN.search(message):
            return Chain.SOLANA
        text = message.lower()
        for keyword, chain in _CHAIN_KEYWORDS.items():
            if keyword in text:
                return chain
        return Chain.CROSS_CHAIN

    @staticmethod
    def _extract_token(message: str) -> str:
        match = _TOKEN_SYMBOL_PATTERN.search(message)
        if match:
            return match.group(1).upper()
        return ""

    @staticmethod
    def _extract_address(message: str) -> str:
        eth = _ETH_PATTERN.search(message)
        if eth:
            return eth.group(0)
        b58 = _BASE58_PATTERN.search(message)
        if b58:
            return b58.group(0)
        return ""

    @staticmethod
    def _extract_time_range(message: str) -> int:
        match = _TIME_RANGE_PATTERN.search(message)
        if match:
            return int(match.group(1))
        return 0
