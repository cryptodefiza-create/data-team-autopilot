from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

_WELCOME_MESSAGE = """Welcome to Data Team Autopilot! Here's what you can try:

**MODE 1 — Just Ask** (no setup needed):
- "Show me the top 50 holders of $BONK"
- "What's the TVL of Aave?"
- "Give me PEPE price history for 30 days"

**MODE 1 — Connect Your Accounts:**
- "Connect my Shopify store"
- "What's my revenue by category this month?"

**MODE 2 — Connect Your Database:**
- "Connect to my BigQuery project"
- "Build me a retention dashboard"
- "Set up weekly memos for my team"

Try Mode 1 first — no setup required!"""

_MODE1_EXAMPLES = [
    "Show me the top 50 holders of $BONK",
    "What's the price of ETH over the last 30 days?",
    "What's the TVL of Aave?",
    "Show me DEX pairs for $PEPE",
]

_MODE1_CONNECT_EXAMPLES = [
    "Connect my Shopify store",
    "What's my revenue by category this month?",
    "Show me orders from last week",
    "Track my daily orders",
]

_MODE2_EXAMPLES = [
    "Connect to my BigQuery project",
    "How many users signed up last week?",
    "Build me a retention dashboard",
    "Set up weekly memos for my team",
]


class OnboardingFlow:
    """Manages the tester onboarding experience."""

    def __init__(self) -> None:
        self._sessions: dict[str, dict[str, Any]] = {}  # org_id -> session state

    def start(self, org_id: str, user_id: str = "") -> dict[str, Any]:
        """Start onboarding for a new user."""
        self._sessions[org_id] = {
            "user_id": user_id,
            "step": "welcome",
            "completed_steps": [],
            "mode_tried": set(),
        }

        return {
            "status": "started",
            "step": "welcome",
            "message": _WELCOME_MESSAGE,
            "examples": {
                "mode1_blockchain": _MODE1_EXAMPLES,
                "mode1_connect": _MODE1_CONNECT_EXAMPLES,
                "mode2_warehouse": _MODE2_EXAMPLES,
            },
        }

    def record_action(self, org_id: str, action: str) -> dict[str, Any]:
        """Record that a user tried an action during onboarding."""
        session = self._sessions.get(org_id)
        if session is None:
            return {"status": "no_session"}

        session["completed_steps"].append(action)

        if action in ("blockchain", "business_query"):
            session["mode_tried"].add("mode1")
        elif action in ("query", "dashboard", "profile"):
            session["mode_tried"].add("mode2")
        elif action == "connect_source":
            session["mode_tried"].add("connect")

        # Check if onboarding is complete (tried at least one mode)
        if session["mode_tried"]:
            session["step"] = "active"

        return {
            "status": "recorded",
            "action": action,
            "modes_tried": list(session["mode_tried"]),
            "steps_completed": len(session["completed_steps"]),
        }

    def get_next_suggestion(self, org_id: str) -> str:
        """Suggest what to try next based on what the user has done."""
        session = self._sessions.get(org_id)
        if session is None:
            return "Try: 'Show me the top 50 holders of $BONK'"

        tried = session.get("mode_tried", set())
        if "mode1" not in tried:
            return "Try Mode 1: 'Show me the top 50 holders of $BONK'"
        if "connect" not in tried:
            return "Try connecting a source: 'Connect my Shopify store'"
        if "mode2" not in tried:
            return "Try Mode 2: 'Connect to my BigQuery project'"
        return "You've tried all modes! Ask any data question."

    def is_onboarded(self, org_id: str) -> bool:
        session = self._sessions.get(org_id)
        return session is not None and bool(session.get("mode_tried"))

    def get_session(self, org_id: str) -> dict[str, Any] | None:
        return self._sessions.get(org_id)
