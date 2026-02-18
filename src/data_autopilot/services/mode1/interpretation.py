from __future__ import annotations

import logging
import re
from typing import Any

from data_autopilot.services.llm_client import LLMClient
from data_autopilot.services.mode1.models import DataRequest, Entity, Interpretation

logger = logging.getLogger(__name__)

_REDACT_PATTERNS = [
    re.compile(r"sk_live_[a-zA-Z0-9]+"),
    re.compile(r"shpat_[a-zA-Z0-9]+"),
    re.compile(r"xox[bpas]-[a-zA-Z0-9\-]+"),
    re.compile(r"\b[a-fA-F0-9]{64}\b"),  # 64-char hex (private keys, long API keys)
]


def sanitize_prompt(text: str) -> str:
    """Ensures no sensitive data leaks into LLM prompts."""
    for pattern in _REDACT_PATTERNS:
        text = pattern.sub("[REDACTED]", text)
    return text


class InterpretationEngine:
    def __init__(self, llm: LLMClient | None = None) -> None:
        self._llm = llm

    def interpret(
        self,
        data: list[dict[str, Any]],
        request: DataRequest,
        max_rows_for_stats: int = 50,
    ) -> Interpretation:
        stats = self.compute_stats(data[:max_rows_for_stats], request)

        if self._llm and self._llm.is_configured():
            try:
                return self._llm_interpret(stats, request)
            except Exception as exc:
                logger.warning("LLM interpretation failed: %s", exc)

        return Interpretation(
            text=self._fallback_interpret(stats, request),
            stats=stats,
        )

    def _llm_interpret(
        self, stats: dict[str, Any], request: DataRequest
    ) -> Interpretation:
        stats_str = sanitize_prompt(str(stats))
        system_prompt = (
            "You are a blockchain data analyst. Provide 2-3 brief observations "
            "about the data. Rules:\n"
            "- Frame everything as observations ('This shows...' not 'You should...')\n"
            "- If noting a pattern, say 'This pattern is sometimes associated with...'\n"
            "- Never give financial advice\n"
            "- Never include raw data in your response\n"
            "- Keep it under 100 words"
        )
        user_prompt = (
            f'The user asked: "{sanitize_prompt(request.raw_message)}"\n\n'
            f"Summary statistics:\n{stats_str}"
        )
        result = self._llm.generate_json(
            system_prompt=system_prompt, user_prompt=user_prompt
        )
        text = str(result.get("observations", result.get("text", "")))
        return Interpretation(text=text, stats=stats)

    def _fallback_interpret(
        self, stats: dict[str, Any], request: DataRequest
    ) -> str:
        parts: list[str] = []
        total = stats.get("total_records", 0)
        parts.append(f"Dataset contains {total} records.")

        if "top_holders_pct" in stats:
            top_pct = stats["top_holders_pct"]
            parts.append(
                f"Top 10 holders control {top_pct:.1f}% of observed supply."
            )
            if top_pct > 50:
                parts.append(
                    "This concentration level is sometimes associated with "
                    "higher volatility."
                )

        if "price_change_pct" in stats:
            chg = stats["price_change_pct"]
            direction = "increase" if chg > 0 else "decrease"
            parts.append(f"Price shows a {abs(chg):.1f}% {direction} over the period.")

        return " ".join(parts)

    @staticmethod
    def compute_stats(
        data: list[dict[str, Any]], request: DataRequest
    ) -> dict[str, Any]:
        stats: dict[str, Any] = {"total_records": len(data)}
        if not data:
            return stats

        # Token holder concentration
        if request.entity in (Entity.TOKEN_HOLDERS, Entity.TOKEN_BALANCES):
            balances = []
            for row in data:
                for key in ("amount", "balance", "tokenBalance"):
                    val = row.get(key)
                    if val is not None:
                        try:
                            balances.append(float(val))
                        except (ValueError, TypeError):
                            pass
                        break
            if balances:
                balances.sort(reverse=True)
                total_supply = sum(balances)
                if total_supply > 0:
                    top10 = balances[:10]
                    stats["top_holders_pct"] = sum(top10) / total_supply * 100
                    stats["max_balance"] = balances[0]
                    stats["median_balance"] = balances[len(balances) // 2]

        # Price trend
        if request.entity in (Entity.PRICE_HISTORY, Entity.TOKEN_PRICE):
            prices = []
            for row in data:
                val = row.get("price")
                if val is not None:
                    try:
                        prices.append(float(val))
                    except (ValueError, TypeError):
                        pass
            if len(prices) >= 2:
                stats["price_start"] = prices[0]
                stats["price_end"] = prices[-1]
                stats["price_min"] = min(prices)
                stats["price_max"] = max(prices)
                if prices[0] != 0:
                    stats["price_change_pct"] = (
                        (prices[-1] - prices[0]) / prices[0] * 100
                    )

        return stats
