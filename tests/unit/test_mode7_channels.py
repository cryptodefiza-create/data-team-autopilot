"""Phase 7 tests: Slack and Telegram message handling, export."""

from unittest.mock import MagicMock

from data_autopilot.services.mode1.slack_handler import SlackEvent, SlackHandler
from data_autopilot.services.mode1.telegram_handler import TelegramEvent, TelegramHandler


def test_slack_question_answer() -> None:
    """7.1: 'How many signups last week?' in Slack → agent responds with answer."""
    # Mock conversation service
    mock_conv = MagicMock()
    mock_conv.respond.return_value = {
        "response_type": "business_result",
        "summary": "Found 142 signups last week",
        "data": {
            "records": [{"count": 142}],
            "record_count": 1,
        },
        "warnings": [],
    }

    mock_channel = MagicMock()

    handler = SlackHandler(
        conversation_service=mock_conv,
        channel_service=mock_channel,
    )
    handler.register_workspace("T_TEAM1", "org_slack")

    event = SlackEvent(
        team_id="T_TEAM1",
        channel="C_GENERAL",
        user="U_USER1",
        text="How many signups last week?",
        ts="1234567890.123456",
    )

    result = handler.handle_message(event)

    assert result["status"] == "sent"
    assert result["channel"] == "C_GENERAL"
    assert "142" in result["response"]

    # Verify message was sent to Slack
    mock_channel.send_slack_message.assert_called_once()
    call_args = mock_channel.send_slack_message.call_args
    assert call_args.kwargs["channel"] == "C_GENERAL"


def test_slack_export_xlsx() -> None:
    """7.2: 'Export that as XLSX' in Slack → agent uploads file."""
    handler = SlackHandler()
    handler.register_workspace("T_TEAM1", "org_slack")

    event = SlackEvent(
        team_id="T_TEAM1",
        channel="C_GENERAL",
        user="U_USER1",
        text="Export that as XLSX",
    )

    result = handler.handle_export_request(event)

    assert result["status"] == "export_requested"
    assert result["channel"] == "C_GENERAL"
    assert result["org_id"] == "org_slack"


def test_telegram_question_answer() -> None:
    """7.3: Telegram question → agent responds with answer."""
    mock_conv = MagicMock()
    mock_conv.respond.return_value = {
        "response_type": "business_result",
        "summary": "Revenue last month: $45,000",
        "data": {"records": [{"revenue": 45000}]},
        "warnings": [],
    }

    mock_channel = MagicMock()

    handler = TelegramHandler(
        conversation_service=mock_conv,
        channel_service=mock_channel,
    )
    handler.register_chat("chat_123", "org_tg")

    event = TelegramEvent(
        chat_id="chat_123",
        user_id="user_456",
        text="What was revenue last month?",
    )

    result = handler.handle_message(event)

    assert result["status"] == "sent"
    assert result["chat_id"] == "chat_123"
    assert "45,000" in result["response"]

    mock_channel.send_telegram_message.assert_called_once()
