from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class TelegramEvent:
    """Parsed Telegram update."""

    def __init__(
        self,
        chat_id: str = "",
        user_id: str = "",
        text: str = "",
        message_id: str = "",
    ) -> None:
        self.chat_id = chat_id
        self.user_id = user_id
        self.text = text
        self.message_id = message_id


class TelegramHandler:
    """Handles Telegram messages → agent responses.

    Same pattern as SlackHandler but via Telegram bot API.
    """

    def __init__(
        self,
        conversation_service: Any = None,
        channel_service: Any = None,
    ) -> None:
        self._conversation = conversation_service
        self._channel = channel_service
        self._chat_map: dict[str, str] = {}  # chat_id -> org_id

    def register_chat(self, chat_id: str, org_id: str) -> None:
        """Map a Telegram chat to an org."""
        self._chat_map[chat_id] = org_id

    def get_org_from_chat(self, chat_id: str) -> str | None:
        return self._chat_map.get(chat_id)

    def handle_message(self, event: TelegramEvent) -> dict[str, Any]:
        """Process a Telegram message and return the agent response."""
        org_id = self.get_org_from_chat(event.chat_id)
        if org_id is None:
            return {
                "status": "error",
                "message": "Chat not registered. Use /start to connect.",
            }

        if not event.text.strip():
            return {"status": "ignored", "message": "Empty message"}

        # Process through conversation service
        if self._conversation:
            try:
                result = self._conversation.respond(
                    db=None,
                    tenant_id=org_id,
                    user_id=event.user_id,
                    message=event.text,
                )
            except Exception as exc:
                logger.error("Telegram handler error: %s", exc)
                result = {
                    "response_type": "error",
                    "summary": f"Error: {exc}",
                }
        else:
            result = {
                "response_type": "info",
                "summary": f"Received: {event.text}",
            }

        # Format for Telegram
        formatted = self.format_telegram(result)

        # Send response
        if self._channel:
            self._channel.send_telegram_message(
                chat_id=event.chat_id,
                text=formatted,
            )

        return {
            "status": "sent",
            "chat_id": event.chat_id,
            "response": formatted,
            "result": result,
        }

    @staticmethod
    def format_telegram(result: dict[str, Any]) -> str:
        """Format an agent result for Telegram display."""
        summary = result.get("summary", "")
        data = result.get("data", {})
        response_type = result.get("response_type", "")

        parts = []
        if summary:
            parts.append(summary)

        if response_type in ("business_result", "blockchain_result"):
            records = data.get("records", [])
            if records:
                parts.append(f"{len(records)} records returned")

        return "\n".join(parts) if parts else "Done."
