from __future__ import annotations

from urllib.parse import parse_qs

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session

from data_autopilot.api.state import (
    audit_service,
    channel_integrations_service,
    conversation_service,
    integration_binding_service,
)
from data_autopilot.db.session import get_db


router = APIRouter()


@router.post("/integrations/slack/command")
async def slack_command(request: Request, db: Session = Depends(get_db)) -> dict:
    raw = await request.body()
    ts = request.headers.get("X-Slack-Request-Timestamp", "")
    sig = request.headers.get("X-Slack-Signature", "")
    if not channel_integrations_service.verify_slack_signature(raw, ts, sig):
        raise HTTPException(status_code=401, detail="Invalid Slack signature")

    form = parse_qs(raw.decode("utf-8"))
    text = str(form.get("text", [""])[0]).strip()
    user_id = str(form.get("user_id", ["unknown"])[0])
    team_id = str(form.get("team_id", [""])[0])
    requested_org, prompt = channel_integrations_service.parse_slack_message(text)
    org_id = integration_binding_service.resolve_for_slack(
        db,
        team_id=team_id,
        user_id=user_id,
        requested_org=requested_org,
        default_org=channel_integrations_service.settings.slack_default_org_id,
    )
    if not org_id or not prompt:
        raise HTTPException(status_code=403, detail="Slack identity is not bound to an org")

    result = conversation_service.respond(db=db, tenant_id=org_id, user_id=f"slack:{user_id}", message=prompt)
    reply = channel_integrations_service.format_agent_result(result)
    audit_service.log(
        db,
        tenant_id=org_id,
        event_type="slack_command_processed",
        payload={
            "user_id": user_id,
            "response_type": result.get("response_type"),
            "intent_action": (result.get("meta") or {}).get("intent_action"),
        },
    )
    return {"response_type": "ephemeral", "text": reply}


@router.post("/integrations/slack/events")
async def slack_events(request: Request, db: Session = Depends(get_db)) -> dict:
    raw = await request.body()
    ts = request.headers.get("X-Slack-Request-Timestamp", "")
    sig = request.headers.get("X-Slack-Signature", "")
    if not channel_integrations_service.verify_slack_signature(raw, ts, sig):
        raise HTTPException(status_code=401, detail="Invalid Slack signature")

    payload = channel_integrations_service.parse_slack_event(raw)
    if payload.get("type") == "url_verification":
        return {"challenge": payload.get("challenge", "")}

    if payload.get("type") == "event_callback":
        event = payload.get("event", {})
        if event.get("type") in {"app_mention", "message"} and event.get("subtype") != "bot_message":
            text = str(event.get("text", "")).strip()
            requested_org, prompt = channel_integrations_service.parse_slack_message(text)
            team_id = str(payload.get("team_id", ""))
            user_id = str(event.get("user", "unknown"))
            org_id = integration_binding_service.resolve_for_slack(
                db,
                team_id=team_id,
                user_id=user_id,
                requested_org=requested_org,
                default_org=channel_integrations_service.settings.slack_default_org_id,
            )
            if org_id and prompt:
                channel = str(event.get("channel", ""))
                thread_ts = str(event.get("thread_ts") or event.get("ts") or "")
                result = conversation_service.respond(db=db, tenant_id=org_id, user_id=f"slack:{user_id}", message=prompt)
                reply = channel_integrations_service.format_agent_result(result)
                channel_integrations_service.send_slack_message(channel, reply, thread_ts=thread_ts or None)
                audit_service.log(
                    db,
                    tenant_id=org_id,
                    event_type="slack_event_processed",
                    payload={
                        "user_id": user_id,
                        "event_type": event.get("type"),
                        "intent_action": (result.get("meta") or {}).get("intent_action"),
                    },
                )
    return {"ok": True}


@router.post("/integrations/telegram/webhook")
async def telegram_webhook(request: Request, db: Session = Depends(get_db)) -> dict:
    secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
    if not channel_integrations_service.verify_telegram_secret(secret):
        raise HTTPException(status_code=401, detail="Invalid Telegram webhook secret")

    payload = await request.json()
    message = payload.get("message") or payload.get("edited_message")
    if not isinstance(message, dict):
        return {"ok": True}

    chat = message.get("chat", {})
    chat_id = str(chat.get("id", ""))
    text = str(message.get("text", "")).strip()
    sender = str((message.get("from") or {}).get("id", "unknown"))
    requested_org, prompt = channel_integrations_service.parse_telegram_message(text)
    org_id = integration_binding_service.resolve_for_telegram(
        db,
        chat_id=chat_id,
        user_id=sender,
        requested_org=requested_org,
        default_org=channel_integrations_service.settings.telegram_default_org_id,
    )
    if not org_id or not prompt:
        channel_integrations_service.send_telegram_message(
            chat_id,
            "Telegram identity is not bound to an org. Ask admin to create a binding.",
        )
        return {"ok": True}

    result = conversation_service.respond(db=db, tenant_id=org_id, user_id=f"tg:{sender}", message=prompt)
    reply = channel_integrations_service.format_agent_result(result)
    channel_integrations_service.send_telegram_message(chat_id, reply)
    audit_service.log(
        db,
        tenant_id=org_id,
        event_type="telegram_message_processed",
        payload={
            "sender": sender,
            "chat_id": chat_id,
            "response_type": result.get("response_type"),
            "intent_action": (result.get("meta") or {}).get("intent_action"),
        },
    )
    return {"ok": True}
