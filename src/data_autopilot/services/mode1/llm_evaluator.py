from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class EvalPrompt:
    """A standardized test prompt for LLM evaluation."""

    def __init__(
        self,
        prompt: str,
        category: str,  # "parsing" | "sql_gen" | "interpretation"
        expected: dict[str, Any] | None = None,
    ) -> None:
        self.prompt = prompt
        self.category = category
        self.expected = expected or {}


class EvalScore:
    """Score for a single evaluation."""

    def __init__(
        self,
        provider: str,
        prompt: EvalPrompt,
        correct: bool,
        response: Any = None,
        cost_usd: float = 0.0,
        latency_ms: float = 0.0,
    ) -> None:
        self.provider = provider
        self.prompt = prompt
        self.correct = correct
        self.response = response
        self.cost_usd = cost_usd
        self.latency_ms = latency_ms


class LLMEvaluator:
    """Evaluates LLM providers across parsing, SQL generation, and interpretation.

    Runs standardized test prompts against each provider and scores results.
    In mock mode, uses keyword-based scoring (no actual LLM calls).
    """

    def __init__(self) -> None:
        self._results: list[EvalScore] = []

    def evaluate_parsing(
        self, provider_name: str, parser_fn: Any, prompts: list[EvalPrompt]
    ) -> dict[str, Any]:
        """Evaluate a provider's parsing accuracy."""
        correct = 0
        scores: list[EvalScore] = []

        for prompt in prompts:
            try:
                result = parser_fn(prompt.prompt)
                is_correct = self._check_parsing_result(result, prompt.expected)
                score = EvalScore(
                    provider=provider_name,
                    prompt=prompt,
                    correct=is_correct,
                    response=result,
                )
                scores.append(score)
                if is_correct:
                    correct += 1
            except Exception as exc:
                scores.append(EvalScore(
                    provider=provider_name, prompt=prompt,
                    correct=False, response=str(exc),
                ))

        self._results.extend(scores)
        accuracy = correct / len(prompts) if prompts else 0.0

        return {
            "provider": provider_name,
            "category": "parsing",
            "total": len(prompts),
            "correct": correct,
            "accuracy": accuracy,
            "scores": scores,
        }

    def evaluate_sql_gen(
        self, provider_name: str, generator_fn: Any, prompts: list[EvalPrompt]
    ) -> dict[str, Any]:
        """Evaluate a provider's SQL generation quality."""
        correct = 0
        total_cost = 0.0
        scores: list[EvalScore] = []

        for prompt in prompts:
            try:
                result = generator_fn(prompt.prompt)
                is_correct = self._check_sql_result(result, prompt.expected)
                cost = result.get("cost_usd", 0.0) if isinstance(result, dict) else 0.0
                total_cost += cost
                score = EvalScore(
                    provider=provider_name, prompt=prompt,
                    correct=is_correct, response=result, cost_usd=cost,
                )
                scores.append(score)
                if is_correct:
                    correct += 1
            except Exception as exc:
                scores.append(EvalScore(
                    provider=provider_name, prompt=prompt,
                    correct=False, response=str(exc),
                ))

        self._results.extend(scores)
        accuracy = correct / len(prompts) if prompts else 0.0

        return {
            "provider": provider_name,
            "category": "sql_gen",
            "total": len(prompts),
            "correct": correct,
            "accuracy": accuracy,
            "total_cost_usd": total_cost,
            "cost_per_query": total_cost / len(prompts) if prompts else 0.0,
            "scores": scores,
        }

    def compare_providers(self) -> dict[str, Any]:
        """Compare all evaluated providers and identify the best."""
        provider_stats: dict[str, dict[str, Any]] = {}

        for score in self._results:
            if score.provider not in provider_stats:
                provider_stats[score.provider] = {
                    "total": 0, "correct": 0, "total_cost": 0.0,
                }
            stats = provider_stats[score.provider]
            stats["total"] += 1
            if score.correct:
                stats["correct"] += 1
            stats["total_cost"] += score.cost_usd

        rankings: list[dict[str, Any]] = []
        for provider, stats in provider_stats.items():
            accuracy = stats["correct"] / stats["total"] if stats["total"] > 0 else 0.0
            rankings.append({
                "provider": provider,
                "accuracy": accuracy,
                "total": stats["total"],
                "correct": stats["correct"],
                "total_cost": stats["total_cost"],
            })

        rankings.sort(key=lambda x: x["accuracy"], reverse=True)

        return {
            "rankings": rankings,
            "best_provider": rankings[0]["provider"] if rankings else None,
            "total_evaluations": len(self._results),
        }

    @staticmethod
    def _check_parsing_result(result: Any, expected: dict[str, Any]) -> bool:
        """Check if parsing result matches expected values."""
        if not expected:
            return result is not None

        if isinstance(result, dict):
            for key, value in expected.items():
                if str(result.get(key, "")).lower() != str(value).lower():
                    return False
            return True

        # Check DataRequest attributes
        for key, value in expected.items():
            actual = getattr(result, key, None)
            if actual is None:
                return False
            if hasattr(actual, "value"):
                actual = actual.value
            if str(actual).lower() != str(value).lower():
                return False
        return True

    @staticmethod
    def _check_sql_result(result: Any, expected: dict[str, Any]) -> bool:
        """Check if SQL generation result is valid."""
        if not expected:
            return result is not None

        if isinstance(result, dict):
            sql = str(result.get("sql", "")).upper()
        elif hasattr(result, "sql"):
            sql = str(result.sql).upper()
        else:
            return False

        # Check expected keywords are present
        for keyword in expected.get("contains", []):
            if keyword.upper() not in sql:
                return False

        # Check SQL is SELECT-only
        if expected.get("select_only", True):
            first_word = sql.strip().split()[0] if sql.strip() else ""
            if first_word not in ("SELECT", "WITH"):
                return False

        return True
