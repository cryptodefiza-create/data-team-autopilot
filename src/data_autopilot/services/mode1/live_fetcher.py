from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from data_autopilot.services.mode1.conversation_memory import ConversationMemory
from data_autopilot.services.mode1.csv_generator import CSVGenerator
from data_autopilot.services.mode1.data_transformer import DataTransformer
from data_autopilot.services.mode1.interpretation import InterpretationEngine
from data_autopilot.services.mode1.models import (
    DataRequest,
    Entity,
    Intent,
    OutputFormat,
    Provenance,
    ProviderResult,
    RoutingMode,
)
from data_autopilot.services.mode1.platform_keys import PlatformKeyManager
from data_autopilot.services.mode1.request_parser import RequestParser
from data_autopilot.services.mode1.request_router import RequestRouter
from data_autopilot.services.mode1.response_formatter import ResponseFormatter
from data_autopilot.services.mode1.source_registry import lookup_fallback
from data_autopilot.services.mode1.xlsx_generator import XLSXGenerator
from data_autopilot.services.providers.base import BaseProvider

logger = logging.getLogger(__name__)

_TIER_LIMITS = {
    "free": 100,
    "starter": 1_000,
    "pro": 10_000,
}

_PROVIDER_SOURCE_NAMES = {
    "helius": "Helius RPC (Solana mainnet)",
    "alchemy": "Alchemy RPC (Ethereum mainnet)",
    "coingecko": "CoinGecko API",
    "dexscreener": "DexScreener API",
    "defillama": "DefiLlama API",
}


class LiveFetcher:
    def __init__(
        self,
        providers: dict[str, BaseProvider],
        key_manager: PlatformKeyManager,
        parser: RequestParser,
        tier: str = "free",
        interpreter: InterpretationEngine | None = None,
    ) -> None:
        self._providers = providers
        self._key_manager = key_manager
        self._parser = parser
        self._router = RequestRouter()
        self._formatter = ResponseFormatter()
        self._xlsx = XLSXGenerator()
        self._csv = CSVGenerator()
        self._transformer = DataTransformer()
        self._interpreter = interpreter or InterpretationEngine()
        self._tier = tier
        # Per-session memory (keyed by session_id externally if needed)
        self._memories: dict[str, ConversationMemory] = {}

    def _get_memory(self, session_id: str = "default") -> ConversationMemory:
        if session_id not in self._memories:
            self._memories[session_id] = ConversationMemory()
        return self._memories[session_id]

    @property
    def record_limit(self) -> int:
        return _TIER_LIMITS.get(self._tier, 100)

    def handle(
        self,
        message: str,
        session_id: str = "default",
        filters: dict[str, Any] | None = None,
        output_format: OutputFormat | None = None,
    ) -> dict[str, Any]:
        memory = self._get_memory(session_id)
        request = self._parser.parse(message)

        # Override output format if explicitly requested
        if output_format:
            request = request.model_copy(update={"output_format": output_format})

        # Detect export/follow-up intent from keywords
        text_lower = message.lower()
        if any(kw in text_lower for kw in ("export", "spreadsheet", "xlsx", "download")):
            request = request.model_copy(
                update={"intent": Intent.EXPORT, "output_format": OutputFormat.XLSX}
            )
        elif "csv" in text_lower:
            request = request.model_copy(
                update={"intent": Intent.EXPORT, "output_format": OutputFormat.CSV}
            )

        # Handle export of previous data
        if request.intent == Intent.EXPORT and memory.has_history:
            prev_data = memory.get_previous_data()
            prev_req = memory.get_previous_request()
            if prev_data is not None and prev_req is not None:
                if filters:
                    prev_data = self._transformer.filter(prev_data, filters)
                return self._format_export(prev_data, prev_req, request.output_format)

        # Handle follow-up filters on previous data
        if filters and memory.has_history:
            prev_data = memory.get_previous_data()
            prev_req = memory.get_previous_request()
            if prev_data is not None and prev_req is not None:
                filtered = self._transformer.filter(prev_data, filters)
                memory.store_result(prev_req, filtered)
                provenance = self._build_provenance(
                    "filtered_previous", prev_req, len(filtered), False
                )
                provenance.filters = filters
                return self._formatter.format_rich(
                    records=filtered,
                    provenance=provenance,
                    output_format=request.output_format,
                )

        # Normal fetch flow
        return self.execute(request, session_id=session_id)

    def execute(
        self, request: DataRequest, session_id: str = "default"
    ) -> dict[str, Any]:
        memory = self._get_memory(session_id)
        decision = self._router.route(request)

        if decision.mode == RoutingMode.ASK_USER:
            return {
                "response_type": "ask_user",
                "summary": decision.reason,
                "data": {"confidence": decision.confidence},
                "warnings": [],
            }

        provider = self._providers.get(decision.provider_name)
        if provider is None:
            return {
                "response_type": "error",
                "summary": f"Provider '{decision.provider_name}' not registered.",
                "data": {},
                "warnings": ["missing_provider"],
            }

        params: dict[str, Any] = {}
        if request.token:
            params["token"] = request.token
        if request.address:
            # For token holder/balance queries, the address is the mint address
            if request.entity in (Entity.TOKEN_HOLDERS, Entity.TOKEN_BALANCES):
                params["mint"] = request.address
            else:
                params["address"] = request.address
        if not params.get("mint") and request.token:
            params["mint"] = request.token
        if request.time_range_days:
            params["days"] = request.time_range_days

        # Resolve token symbol → mint address for Helius queries
        if (
            decision.provider_name == "helius"
            and params.get("mint")
            and not self._looks_like_address(params["mint"])
        ):
            resolved = self._resolve_token_address(params["mint"])
            if resolved:
                logger.info("Resolved %s → %s", params["mint"], resolved)
                params["mint"] = resolved
            else:
                return {
                    "response_type": "error",
                    "summary": f"Could not resolve token symbol '{params['mint']}' to a mint address.",
                    "data": {},
                    "warnings": ["token_resolution_failed"],
                }

        result = provider.fetch(decision.method_name, params)

        # Fallback logic: if primary provider fails, try fallback
        if not result.succeeded:
            fallback = lookup_fallback(decision.provider_name, decision.method_name)
            if fallback:
                fb_provider_name, fb_method = fallback
                fb_provider = self._providers.get(fb_provider_name)
                if fb_provider:
                    logger.info(
                        "Primary %s failed, falling back to %s",
                        decision.provider_name,
                        fb_provider_name,
                    )
                    result = fb_provider.fetch(fb_method, params)

        # Enforce tier record limit
        truncated = False
        if result.succeeded and len(result.records) > self.record_limit:
            result = ProviderResult(
                provider=result.provider,
                method=result.method,
                records=result.records[: self.record_limit],
                total_available=result.total_available,
                truncated=True,
            )
            truncated = True

        if not result.succeeded:
            return self._formatter.format(result, request.output_format)

        # Store in memory for follow-ups
        memory.store_result(request, result.records)

        # Build provenance
        provenance = self._build_provenance(
            result.provider, request, result.total_available, truncated
        )

        # Compute interpretation (sync, LLM optional)
        interpretation = self._interpreter.interpret(result.records, request)

        # Handle export formats
        if request.output_format in (OutputFormat.XLSX, OutputFormat.CSV):
            return self._format_export(result.records, request, request.output_format)

        return self._formatter.format_rich(
            records=result.records,
            provenance=provenance,
            interpretation=interpretation,
            output_format=request.output_format,
            truncated=truncated,
            total_available=result.total_available,
        )

    def _build_provenance(
        self,
        provider_name: str,
        request: DataRequest,
        total_count: int,
        truncated: bool,
    ) -> Provenance:
        source = _PROVIDER_SOURCE_NAMES.get(provider_name, provider_name)
        return Provenance(
            source=source,
            timestamp=datetime.now(timezone.utc),
            chain=request.chain.value if request.chain else None,
            params={"token": request.token, "address": request.address},
            record_count=total_count,
            truncated=truncated,
            sampling_note=(
                f"Showing top {self.record_limit} by default"
                if truncated
                else None
            ),
        )

    @staticmethod
    def _looks_like_address(value: str) -> bool:
        """Return True if value looks like a Solana or Ethereum address."""
        if len(value) >= 32 and all(c.isalnum() for c in value):
            return True
        if value.startswith("0x") and len(value) == 42:
            return True
        return False

    def _resolve_token_address(self, symbol: str) -> str | None:
        """Use DexScreener to resolve a token symbol to its mint address."""
        dex = self._providers.get("dexscreener")
        if not dex:
            return None
        try:
            result = dex.fetch("search_pairs", {"query": symbol})
            if result.succeeded and result.records:
                # Find a Solana pair matching the symbol
                for record in result.records:
                    if record.get("chain") == "solana" and record.get("base_token", "").upper() == symbol.upper():
                        # DexScreener search returns pair info; need to get base token address
                        # Use the raw API response instead
                        break
                # Fallback: use DexScreener search API directly for token address
                import httpx
                resp = httpx.get(
                    "https://api.dexscreener.com/latest/dex/search",
                    params={"q": symbol},
                    timeout=10,
                )
                resp.raise_for_status()
                pairs = resp.json().get("pairs", [])
                for p in pairs:
                    if (
                        p.get("chainId") == "solana"
                        and p.get("baseToken", {}).get("symbol", "").upper() == symbol.upper()
                    ):
                        return p["baseToken"]["address"]
        except Exception as exc:
            logger.warning("Token resolution for %s failed: %s", symbol, exc)
        return None

    def _format_export(
        self,
        data: list[dict[str, Any]],
        request: DataRequest,
        output_format: OutputFormat,
    ) -> dict[str, Any]:
        provenance = self._build_provenance(
            "export", request, len(data), False
        )
        if output_format == OutputFormat.XLSX:
            xlsx_bytes = self._xlsx.generate(data, provenance)
            return {
                "response_type": "blockchain_export",
                "summary": f"Generated XLSX with {len(data)} records.",
                "data": {
                    "format": "xlsx",
                    "content_bytes": xlsx_bytes,
                    "record_count": len(data),
                },
                "warnings": [],
            }
        else:
            csv_str = self._csv.generate(data, provenance)
            return {
                "response_type": "blockchain_export",
                "summary": f"Generated CSV with {len(data)} records.",
                "data": {
                    "format": "csv",
                    "content": csv_str,
                    "record_count": len(data),
                },
                "warnings": [],
            }
