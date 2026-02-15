import hashlib
import hmac
import json
import time
from urllib.parse import urlencode

from fastapi.testclient import TestClient

from data_autopilot.api.state import channel_integrations_service
from data_autopilot.main import app


client = TestClient(app)


def _slack_signature(secret: str, timestamp: str, body: bytes) -> str:
    base = f"v0:{timestamp}:{body.decode('utf-8')}".encode("utf-8")
    digest = hmac.new(secret.encode("utf-8"), base, hashlib.sha256).hexdigest()
    return f"v0={digest}"


def test_app_shell_hotkey_page() -> None:
    r = client.get("/app")
    assert r.status_code == 200
    assert "Cmd/Ctrl + K" in r.text
    assert "/api/v1/agent/run" in r.text


def test_slack_command_rejects_invalid_signature() -> None:
    settings = channel_integrations_service.settings
    old_secret = settings.slack_signing_secret
    old_org = settings.slack_default_org_id
    settings.slack_signing_secret = "test-secret"
    settings.slack_default_org_id = "org_slack_sig"
    try:
        body = urlencode({"user_id": "U1", "text": "show me dau"}).encode("utf-8")
        r = client.post(
            "/integrations/slack/command",
            content=body,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "X-Slack-Request-Timestamp": str(int(time.time())),
                "X-Slack-Signature": "v0=bad",
            },
        )
    finally:
        settings.slack_signing_secret = old_secret
        settings.slack_default_org_id = old_org
    assert r.status_code == 401


def test_slack_command_accepts_valid_signature_and_blocks_replay() -> None:
    settings = channel_integrations_service.settings
    old_secret = settings.slack_signing_secret
    old_org = settings.slack_default_org_id
    settings.slack_signing_secret = "test-secret"
    settings.slack_default_org_id = "org_slack_cmd"
    try:
        ts = str(int(time.time()))
        body = urlencode({"user_id": "U123", "text": "show me dau"}).encode("utf-8")
        sig = _slack_signature(settings.slack_signing_secret, ts, body)
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "X-Slack-Request-Timestamp": ts,
            "X-Slack-Signature": sig,
        }
        first = client.post("/integrations/slack/command", content=body, headers=headers)
        second = client.post("/integrations/slack/command", content=body, headers=headers)
    finally:
        settings.slack_signing_secret = old_secret
        settings.slack_default_org_id = old_org
    assert first.status_code == 200
    assert "text" in first.json()
    assert second.status_code == 401


def test_slack_events_url_verification_and_app_mention() -> None:
    settings = channel_integrations_service.settings
    old_secret = settings.slack_signing_secret
    old_org = settings.slack_default_org_id
    settings.slack_signing_secret = "test-secret"
    settings.slack_default_org_id = "org_slack_events"
    sent: list[dict] = []
    old_send = channel_integrations_service.send_slack_message
    channel_integrations_service.send_slack_message = lambda channel, text, thread_ts=None: sent.append(
        {"channel": channel, "text": text, "thread_ts": thread_ts}
    )
    try:
        challenge_payload = {"type": "url_verification", "challenge": "abc123"}
        raw_challenge = json.dumps(challenge_payload).encode("utf-8")
        ts1 = str(int(time.time()))
        sig1 = _slack_signature(settings.slack_signing_secret, ts1, raw_challenge)
        verify = client.post(
            "/integrations/slack/events",
            content=raw_challenge,
            headers={
                "Content-Type": "application/json",
                "X-Slack-Request-Timestamp": ts1,
                "X-Slack-Signature": sig1,
            },
        )

        event_payload = {
            "type": "event_callback",
            "event": {
                "type": "app_mention",
                "user": "U777",
                "channel": "C999",
                "ts": "123.456",
                "text": "<@BOT> show me dau",
            },
        }
        raw_event = json.dumps(event_payload).encode("utf-8")
        ts2 = str(int(time.time()) + 1)
        sig2 = _slack_signature(settings.slack_signing_secret, ts2, raw_event)
        evt = client.post(
            "/integrations/slack/events",
            content=raw_event,
            headers={
                "Content-Type": "application/json",
                "X-Slack-Request-Timestamp": ts2,
                "X-Slack-Signature": sig2,
            },
        )
    finally:
        channel_integrations_service.send_slack_message = old_send
        settings.slack_signing_secret = old_secret
        settings.slack_default_org_id = old_org
    assert verify.status_code == 200
    assert verify.json()["challenge"] == "abc123"
    assert evt.status_code == 200
    assert sent and sent[0]["channel"] == "C999"


def test_telegram_webhook_secret_and_message_handling() -> None:
    settings = channel_integrations_service.settings
    old_secret = settings.telegram_webhook_secret
    old_org = settings.telegram_default_org_id
    settings.telegram_webhook_secret = "tg-secret"
    settings.telegram_default_org_id = "org_tg"
    sent: list[dict] = []
    old_send = channel_integrations_service.send_telegram_message
    channel_integrations_service.send_telegram_message = lambda chat_id, text: sent.append(
        {"chat_id": chat_id, "text": text}
    )
    payload = {
        "message": {
            "chat": {"id": 555},
            "from": {"id": 777},
            "text": "/ask show me dau",
        }
    }
    try:
        bad = client.post(
            "/integrations/telegram/webhook",
            json=payload,
            headers={"X-Telegram-Bot-Api-Secret-Token": "wrong"},
        )
        good = client.post(
            "/integrations/telegram/webhook",
            json=payload,
            headers={"X-Telegram-Bot-Api-Secret-Token": "tg-secret"},
        )
    finally:
        channel_integrations_service.send_telegram_message = old_send
        settings.telegram_webhook_secret = old_secret
        settings.telegram_default_org_id = old_org
    assert bad.status_code == 401
    assert good.status_code == 200
    assert sent and sent[0]["chat_id"] == "555"


def test_slack_team_binding_enforced_for_org_override() -> None:
    settings = channel_integrations_service.settings
    old_secret = settings.slack_signing_secret
    old_org = settings.slack_default_org_id
    settings.slack_signing_secret = "bind-secret"
    settings.slack_default_org_id = ""
    try:
        admin = {"X-Tenant-Id": "org_bind_a", "X-User-Role": "admin"}
        create = client.post(
            "/api/v1/integrations/bindings",
            json={"org_id": "org_bind_a", "binding_type": "slack_team", "external_id": "T111"},
            headers=admin,
        )
        assert create.status_code == 200

        body = urlencode({"team_id": "T111", "user_id": "U1", "text": "org:org_bind_b show me dau"}).encode("utf-8")
        ts = str(int(time.time()))
        sig = _slack_signature(settings.slack_signing_secret, ts, body)
        bad = client.post(
            "/integrations/slack/command",
            content=body,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "X-Slack-Request-Timestamp": ts,
                "X-Slack-Signature": sig,
            },
        )
        assert bad.status_code == 403

        body2 = urlencode({"team_id": "T111", "user_id": "U1", "text": "show me dau"}).encode("utf-8")
        ts2 = str(int(time.time()) + 1)
        sig2 = _slack_signature(settings.slack_signing_secret, ts2, body2)
        ok = client.post(
            "/integrations/slack/command",
            content=body2,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "X-Slack-Request-Timestamp": ts2,
                "X-Slack-Signature": sig2,
            },
        )
        assert ok.status_code == 200
    finally:
        settings.slack_signing_secret = old_secret
        settings.slack_default_org_id = old_org


def test_telegram_chat_binding_enforced_for_org_override() -> None:
    settings = channel_integrations_service.settings
    old_secret = settings.telegram_webhook_secret
    old_org = settings.telegram_default_org_id
    settings.telegram_webhook_secret = "tg-bind"
    settings.telegram_default_org_id = ""
    sent: list[dict] = []
    old_send = channel_integrations_service.send_telegram_message
    channel_integrations_service.send_telegram_message = lambda chat_id, text: sent.append(
        {"chat_id": chat_id, "text": text}
    )
    try:
        admin = {"X-Tenant-Id": "org_tg_bind", "X-User-Role": "admin"}
        create = client.post(
            "/api/v1/integrations/bindings",
            json={"org_id": "org_tg_bind", "binding_type": "telegram_chat", "external_id": "555"},
            headers=admin,
        )
        assert create.status_code == 200

        payload = {"message": {"chat": {"id": 555}, "from": {"id": 123}, "text": "/ask org:other show me dau"}}
        resp = client.post(
            "/integrations/telegram/webhook",
            json=payload,
            headers={"X-Telegram-Bot-Api-Secret-Token": "tg-bind"},
        )
        assert resp.status_code == 200
        assert sent and "not bound" in sent[-1]["text"]
    finally:
        channel_integrations_service.send_telegram_message = old_send
        settings.telegram_webhook_secret = old_secret
        settings.telegram_default_org_id = old_org
