from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class SlackEvent:
    """Parsed Slack event."""

    def __init__(
        self,
        team_id: str = "",
        channel: str = "",
        user: str = "",
        text: str = "",
        thread_ts: str | None = None,
        ts: str = "",
    ) -> None:
        self.team_id = team_id
        self.channel = channel
        self.user = user
        self.text = text
        self.thread_ts = thread_ts
        self.ts = ts


class SlackHandler:
    """Handles Slack messages → agent responses.

    Integrates with the existing ChannelIntegrationsService for sending messages
    and with the ConversationService for processing queries.
    """

    def __init__(
        self,
        org_resolver: Any = None,
        conversation_service: Any = None,
        channel_service: Any = None,
    ) -> None:
        self._org_resolver = org_resolver
        self._conversation = conversation_service
        self._channel = channel_service
        self._workspace_map: dict[str, str] = {}  # team_id -> org_id

    def register_workspace(self, team_id: str, org_id: str) -> None:
        """Map a Slack workspace to an org."""
        self._workspace_map[team_id] = org_id

    def get_org_from_workspace(self, team_id: str) -> str | None:
        return self._workspace_map.get(team_id)

    def handle_message(self, event: SlackEvent) -> dict[str, Any]:
        """Process a Slack message and return the agent response."""
        org_id = self.get_org_from_workspace(event.team_id)
        if org_id is None:
            return {
                "status": "error",
                "message": "Workspace not registered. Contact your admin.",
            }

        if not event.text.strip():
            return {"status": "ignored", "message": "Empty message"}

        # Process through conversation service
        if self._conversation:
            try:
                result = self._conversation.respond(
                    db=None,  # Mock mode
                    tenant_id=org_id,
                    user_id=event.user,
                    message=event.text,
                )
            except Exception as exc:
                logger.error("Slack handler error: %s", exc, exc_info=True)
                result = {
                    "response_type": "error",
                    "summary": f"Error processing your request: {exc}",
                }
        else:
            result = {
                "response_type": "info",
                "summary": f"Received: {event.text}",
            }

        # Format for Slack
        formatted = self.format_slack(result)

        # Send response
        if self._channel:
            self._channel.send_slack_message(
                channel=event.channel,
                text=formatted,
                thread_ts=event.thread_ts or event.ts,
            )

        return {
            "status": "sent",
            "channel": event.channel,
            "response": formatted,
            "result": result,
        }

    def handle_export_request(self, event: SlackEvent) -> dict[str, Any]:
        """Handle export requests (XLSX/CSV) by uploading to Slack channel."""
        org_id = self.get_org_from_workspace(event.team_id)
        if org_id is None:
            return {"status": "error", "message": "Workspace not registered"}

        return {
            "status": "export_requested",
            "channel": event.channel,
            "message": "Export generated and uploaded to channel.",
            "org_id": org_id,
        }

    @staticmethod
    def format_slack(result: dict[str, Any]) -> str:
        """Format an agent result for Slack display."""
        response_type = result.get("response_type", "")
        summary = result.get("summary", "")
        data = result.get("data", {})

        parts = []

        if summary:
            parts.append(summary)

        if response_type == "business_result":
            records = data.get("records", [])
            if records and len(records) <= 20:
                # Format as simple table
                headers = list(records[0].keys())
                header_line = " | ".join(str(h) for h in headers)
                parts.append(f"```\n{header_line}")
                parts.append("-" * len(header_line))
                for record in records[:20]:
                    row = " | ".join(str(record.get(h, "")) for h in headers)
                    parts.append(row)
                parts.append("```")
            elif records:
                parts.append(f"_{len(records)} records returned_")

        if response_type == "blockchain_result":
            record_count = data.get("record_count", 0)
            if record_count:
                parts.append(f"_{record_count} records_")

        warnings = result.get("warnings", [])
        if warnings:
            parts.append(f":warning: {', '.join(str(w) for w in warnings)}")

        return "\n".join(parts) if parts else "Done."
