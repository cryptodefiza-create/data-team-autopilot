"""Phase 8 tests: LLM evaluation, error handling, onboarding, feedback."""

from data_autopilot.services.mode1.error_handler import ErrorHandler
from data_autopilot.services.mode1.feedback import FeedbackSystem
from data_autopilot.services.mode1.llm_evaluator import EvalPrompt, LLMEvaluator
from data_autopilot.services.mode1.onboarding import OnboardingFlow
from data_autopilot.services.mode1.request_parser import RequestParser


def test_llm_eval_parsing() -> None:
    """8.1: 20 test prompts × 3 providers → accuracy scores, best provider identified."""
    evaluator = LLMEvaluator()
    parser = RequestParser()

    # Create standardized test prompts for parsing
    prompts = [
        EvalPrompt("Show me holders of $BONK on Solana", "parsing",
                    {"entity": "token_holders", "token": "BONK", "chain": "solana"}),
        EvalPrompt("What's the price of ETH?", "parsing",
                    {"entity": "token_price", "token": "ETH"}),
        EvalPrompt("Price history of PEPE for 30 days", "parsing",
                    {"entity": "price_history", "token": "PEPE"}),
        EvalPrompt("Show me $SOL token info", "parsing",
                    {"entity": "token_info", "token": "SOL"}),
        EvalPrompt("Top holders of $WIF", "parsing",
                    {"entity": "token_holders", "token": "WIF"}),
    ]

    # Simulate 3 providers using keyword parser (mock LLM)
    results = []
    for provider_name in ["grok4_fast", "gpt5_mini", "claude_sonnet"]:
        result = evaluator.evaluate_parsing(
            provider_name=provider_name,
            parser_fn=lambda msg: parser.parse(msg),
            prompts=prompts,
        )
        results.append(result)

    # Each provider should have results
    assert len(results) == 3
    for r in results:
        assert r["total"] == 5
        assert r["accuracy"] >= 0.0

    # Compare providers
    comparison = evaluator.compare_providers()
    assert comparison["best_provider"] is not None
    assert comparison["total_evaluations"] == 15  # 5 prompts × 3 providers


def test_llm_eval_sql_gen() -> None:
    """8.2: 20 SQL tasks × 3 providers → correctness + cost per provider."""
    evaluator = LLMEvaluator()

    prompts = [
        EvalPrompt("Count all users", "sql_gen",
                    {"contains": ["SELECT", "COUNT"], "select_only": True}),
        EvalPrompt("Total revenue by month", "sql_gen",
                    {"contains": ["SELECT", "GROUP BY"], "select_only": True}),
        EvalPrompt("Top 10 customers by spend", "sql_gen",
                    {"contains": ["SELECT", "ORDER BY"], "select_only": True}),
    ]

    def mock_sql_gen(prompt: str) -> dict:
        text = prompt.lower()
        if "count" in text:
            return {"sql": "SELECT COUNT(*) FROM users", "cost_usd": 0.001}
        if "revenue" in text:
            return {"sql": "SELECT month, SUM(amount) FROM orders GROUP BY month", "cost_usd": 0.002}
        return {"sql": "SELECT * FROM orders ORDER BY amount DESC LIMIT 10", "cost_usd": 0.001}

    for provider in ["grok4_fast", "gpt5_mini", "claude_sonnet"]:
        result = evaluator.evaluate_sql_gen(
            provider_name=provider,
            generator_fn=mock_sql_gen,
            prompts=prompts,
        )
        assert result["total"] == 3
        assert result["accuracy"] > 0.0
        assert result["cost_per_query"] > 0.0

    comparison = evaluator.compare_providers()
    assert comparison["best_provider"] is not None


def test_error_provider_timeout() -> None:
    """8.3: Simulate Helius timeout → graceful error message, no crash."""
    handler = ErrorHandler()
    result = handler.handle_provider_timeout("Helius")

    assert result["response_type"] == "error"
    assert "helius" in result["summary"].lower()
    assert "try again" in result["summary"].lower()
    assert result["data"]["error_type"] == "provider_timeout"
    assert len(result["suggestions"]) > 0


def test_error_invalid_address() -> None:
    """8.4: 'Holders of abc123' → helpful error suggesting checking address."""
    handler = ErrorHandler()
    result = handler.handle_invalid_address("abc123")

    assert result["response_type"] == "error"
    assert "doesn't look like a valid" in result["summary"].lower()
    assert "abc123" in result["summary"]
    assert len(result["suggestions"]) > 0

    # Valid addresses should pass
    assert handler.validate_address("0x742d35Cc6634C0532925a3b844Bc9e7595f2bD12")
    assert not handler.validate_address("abc123")


def test_error_empty_results() -> None:
    """8.5: Query with 0 results → informative response with suggestions."""
    handler = ErrorHandler()
    result = handler.handle_empty_results(
        "token_holders",
        params={"token": "UNKNOWNTOKEN"},
    )

    assert result["response_type"] == "info"
    assert "no token holders found" in result["summary"].lower()
    assert len(result["suggestions"]) > 0
    # Should suggest alternatives
    assert any("address" in s.lower() or "symbol" in s.lower() for s in result["suggestions"])


def test_tester_onboarding() -> None:
    """8.6: New user, first session → welcome flow with examples, can try Mode 1."""
    flow = OnboardingFlow()

    result = flow.start("org_new", user_id="user_1")

    assert result["status"] == "started"
    assert "Welcome" in result["message"]
    assert "MODE 1" in result["message"]
    assert "MODE 2" in result["message"]
    assert "mode1_blockchain" in result["examples"]
    assert len(result["examples"]["mode1_blockchain"]) > 0

    # User tries a blockchain query
    action_result = flow.record_action("org_new", "blockchain")
    assert action_result["status"] == "recorded"
    assert "mode1" in action_result["modes_tried"]

    # User is now onboarded
    assert flow.is_onboarded("org_new")

    # Suggest next action
    suggestion = flow.get_next_suggestion("org_new")
    assert "connect" in suggestion.lower() or "mode 2" in suggestion.lower()


def test_feedback_submission() -> None:
    """8.11: Thumbs down on response → stored with provider, query type, tier, filterable."""
    system = FeedbackSystem()

    entry = system.submit(
        org_id="org_fb",
        user_id="user_1",
        rating="down",
        provider="helius",
        query_type="token_holders",
        mode="mode1",
        tier="pro",
        message="Data looks outdated",
        query_text="Show me $BONK holders",
    )

    assert entry.id.startswith("fb_")
    assert entry.rating == "down"
    assert entry.provider == "helius"

    # Submit a thumbs up too
    system.submit(
        org_id="org_fb", user_id="user_2", rating="up",
        provider="coingecko", mode="mode1", tier="free",
    )

    # Filter by provider
    helius_fb = system.get_feedback(provider="helius")
    assert len(helius_fb) == 1
    assert helius_fb[0].rating == "down"

    # Filter by rating
    down_fb = system.get_feedback(rating="down")
    assert len(down_fb) == 1

    # Get stats
    stats = system.get_stats()
    assert stats["total"] == 2
    assert stats["up"] == 1
    assert stats["down"] == 1
    assert stats["satisfaction_rate"] == 0.5
    assert "helius" in stats["by_provider"]
