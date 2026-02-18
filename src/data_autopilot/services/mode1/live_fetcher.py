from __future__ import annotations

import logging
from typing import Any

from data_autopilot.services.mode1.models import (
    DataRequest,
    ProviderResult,
    RoutingMode,
)
from data_autopilot.services.mode1.platform_keys import PlatformKeyManager
from data_autopilot.services.mode1.request_parser import RequestParser
from data_autopilot.services.mode1.request_router import RequestRouter
from data_autopilot.services.mode1.response_formatter import ResponseFormatter
from data_autopilot.services.providers.base import BaseProvider

logger = logging.getLogger(__name__)

_TIER_LIMITS = {
    "free": 100,
    "starter": 1_000,
    "pro": 10_000,
}


class LiveFetcher:
    def __init__(
        self,
        providers: dict[str, BaseProvider],
        key_manager: PlatformKeyManager,
        parser: RequestParser,
        tier: str = "free",
    ) -> None:
        self._providers = providers
        self._key_manager = key_manager
        self._parser = parser
        self._router = RequestRouter()
        self._formatter = ResponseFormatter()
        self._tier = tier

    @property
    def record_limit(self) -> int:
        return _TIER_LIMITS.get(self._tier, 100)

    def handle(self, message: str) -> dict[str, Any]:
        request = self._parser.parse(message)
        return self.execute(request)

    def execute(self, request: DataRequest) -> dict[str, Any]:
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

        # Build params from request
        params: dict[str, Any] = {}
        if request.token:
            params["token"] = request.token
            params["mint"] = request.token
        if request.address:
            params["address"] = request.address
        if request.time_range_days:
            params["days"] = request.time_range_days

        result = provider.fetch(decision.method_name, params)

        # Enforce tier record limit
        if result.succeeded and len(result.records) > self.record_limit:
            result = ProviderResult(
                provider=result.provider,
                method=result.method,
                records=result.records[: self.record_limit],
                total_available=result.total_available,
                truncated=True,
            )

        return self._formatter.format(result, request.output_format)
