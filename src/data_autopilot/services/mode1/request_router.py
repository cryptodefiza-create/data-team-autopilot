from __future__ import annotations

import re

from data_autopilot.services.mode1.models import DataRequest, RoutingDecision, RoutingMode
from data_autopilot.services.mode1.source_registry import lookup

_PRIVATE_SIGNALS = re.compile(r"\b(my|our|internal|private|company)\b", re.IGNORECASE)


class RequestRouter:
    def route(self, request: DataRequest) -> RoutingDecision:
        private_penalty = 0.0
        if _PRIVATE_SIGNALS.search(request.raw_message):
            private_penalty = 0.4

        registry_match = lookup(request.chain, request.entity)
        if registry_match is None:
            return RoutingDecision(
                mode=RoutingMode.ASK_USER,
                confidence=0.3,
                reason="No provider registered for this chain/entity combination",
            )

        provider_name, method_name = registry_match
        base_confidence = 0.9
        confidence = max(0.0, base_confidence - private_penalty)

        if confidence < 0.7:
            return RoutingDecision(
                mode=RoutingMode.ASK_USER,
                confidence=confidence,
                provider_name=provider_name,
                method_name=method_name,
                reason="Private signal detected — user may want warehouse data instead",
            )

        return RoutingDecision(
            mode=RoutingMode.PUBLIC_API,
            confidence=confidence,
            provider_name=provider_name,
            method_name=method_name,
            reason="Matched public blockchain data provider",
        )
