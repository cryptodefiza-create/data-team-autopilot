from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

# Base58 pattern (Solana addresses)
_BASE58_PATTERN = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")
# Ethereum address pattern
_ETH_PATTERN = re.compile(r"^0x[a-fA-F0-9]{40}$")


class ErrorHandler:
    """Produces user-friendly error messages for common failure cases.

    Transforms raw errors into helpful, actionable messages.
    """

    def handle_provider_timeout(self, provider: str) -> dict[str, Any]:
        """Handle provider timeout with a friendly message."""
        return {
            "response_type": "error",
            "summary": (
                f"I'm having trouble reaching {provider} right now. "
                f"Try again in a moment?"
            ),
            "data": {"error_type": "provider_timeout", "provider": provider},
            "warnings": ["provider_timeout"],
            "suggestions": [
                "Wait a few seconds and try again",
                f"Check if {provider} is experiencing issues",
            ],
        }

    def handle_invalid_address(self, address: str) -> dict[str, Any]:
        """Handle invalid blockchain address."""
        suggestions = []
        if len(address) < 20:
            suggestions.append("The address seems too short. Solana addresses are 32-44 characters.")
        elif address.startswith("0x") and not _ETH_PATTERN.match(address):
            suggestions.append("This looks like an Ethereum address but is malformed. It should be 0x followed by 40 hex characters.")
        elif not address.startswith("0x") and not _BASE58_PATTERN.match(address):
            suggestions.append("This doesn't look like a valid Solana address. It should be 32-44 base58 characters.")
        else:
            suggestions.append("Double-check the address and try again.")

        return {
            "response_type": "error",
            "summary": (
                f"That doesn't look like a valid blockchain address: '{address}'. "
                "Can you double-check?"
            ),
            "data": {"error_type": "invalid_address", "address": address},
            "warnings": ["invalid_address"],
            "suggestions": suggestions,
        }

    def handle_empty_results(
        self, entity: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Handle queries that return zero results."""
        suggestions = self._suggest_alternatives(entity, params)

        param_desc = ""
        if params:
            param_parts = [f"{k}={v}" for k, v in params.items() if v]
            if param_parts:
                param_desc = f" for {', '.join(param_parts)}"

        return {
            "response_type": "info",
            "summary": (
                f"No {entity.replace('_', ' ')} found{param_desc}. "
                "This could mean the token/address has no activity for this query."
            ),
            "data": {"error_type": "empty_results", "entity": entity},
            "warnings": [],
            "suggestions": suggestions,
        }

    def handle_ambiguous_prompt(
        self, message: str, options: list[str]
    ) -> dict[str, Any]:
        """Handle ambiguous user prompts by asking for clarification."""
        options_text = "\n".join(f"  {i + 1}. {opt}" for i, opt in enumerate(options))
        return {
            "response_type": "clarification",
            "summary": (
                f"I want to make sure I get this right. Did you mean:\n{options_text}"
            ),
            "data": {
                "error_type": "ambiguous_prompt",
                "original_message": message,
                "options": options,
            },
            "warnings": [],
        }

    def validate_address(self, address: str) -> bool:
        """Check if an address looks valid (Solana or Ethereum)."""
        if _ETH_PATTERN.match(address):
            return True
        if _BASE58_PATTERN.match(address):
            return True
        return False

    @staticmethod
    def _suggest_alternatives(entity: str, params: dict[str, Any] | None) -> list[str]:
        """Generate helpful suggestions for empty results."""
        suggestions = []

        if "holder" in entity:
            suggestions.append("The token might have a different contract address.")
            suggestions.append("Try searching by token symbol (e.g., '$BONK').")
        elif "price" in entity:
            suggestions.append("The token may not be listed on major exchanges yet.")
            suggestions.append("Try using the full token name or contract address.")
        elif "transfer" in entity or "transaction" in entity:
            suggestions.append("The address may have no recent transactions.")
            suggestions.append("Try expanding the time range.")
        else:
            suggestions.append("Try adjusting your search parameters.")

        return suggestions
