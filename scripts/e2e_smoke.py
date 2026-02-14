#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys

import httpx


def expect(cond: bool, message: str) -> None:
    if not cond:
        raise RuntimeError(message)


def main() -> int:
    parser = argparse.ArgumentParser(description="Data Team Autopilot E2E smoke test")
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--org-id", default="org_smoke")
    parser.add_argument("--user-id", default="user_smoke")
    parser.add_argument("--in-process", action="store_true", help="Run against FastAPI app in-process (no network sockets)")
    args = parser.parse_args()

    if args.in_process:
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        src_dir = os.path.join(root, "src")
        if src_dir not in sys.path:
            sys.path.insert(0, src_dir)
        from fastapi.testclient import TestClient

        from data_autopilot.main import app

        client = TestClient(app)
        base = ""
    else:
        base = args.base_url.rstrip("/")
        client = httpx.Client(timeout=20)

    with client:
        tenant_headers = {"X-Tenant-Id": args.org_id, "X-User-Role": "admin"}
        health = client.get(f"{base}/health")
        expect(health.status_code == 200, "health check failed")

        ready = client.get(f"{base}/ready")
        expect(ready.status_code == 200, "ready check failed")

        connect = client.post(
            f"{base}/api/v1/connectors/bigquery",
            json={"org_id": args.org_id, "service_account_json": {"client_email": "smoke@example.com"}},
            headers=tenant_headers,
        )
        expect(connect.status_code == 200, "connect failed")
        conn = connect.json()["connection_id"]

        profile = client.post(f"{base}/api/v1/workflows/profile", params={"org_id": args.org_id}, headers=tenant_headers)
        expect(profile.status_code == 200, "profile failed")
        expect(profile.json().get("status") == "success", "profile not successful")

        dashboard = client.post(f"{base}/api/v1/workflows/dashboard", params={"org_id": args.org_id}, headers=tenant_headers)
        expect(dashboard.status_code == 200, "dashboard failed")
        dash = dashboard.json()
        expect(dash.get("status") == "success", "dashboard not successful")
        expect(bool(dash.get("artifact_id")), "dashboard artifact missing")

        memo = client.post(f"{base}/api/v1/workflows/memo", params={"org_id": args.org_id}, headers=tenant_headers)
        expect(memo.status_code == 200, "memo failed")
        memo_payload = memo.json()
        expect(memo_payload.get("status") == "success", "memo not successful")
        expect(memo_payload.get("validation", {}).get("passed") is True, "memo validation failed")

        fb = client.post(
            f"{base}/api/v1/feedback",
            json={
                "tenant_id": args.org_id,
                "user_id": args.user_id,
                "artifact_id": memo_payload["artifact_id"],
                "artifact_version": memo_payload.get("version", 1),
                "artifact_type": "memo",
                "feedback_type": "positive",
                "comment": "smoke test",
                "prompt_hash": "smoke_prompt",
            },
            headers=tenant_headers,
        )
        expect(fb.status_code == 200, "feedback submit failed")

        summary = client.get(f"{base}/api/v1/feedback/summary", params={"org_id": args.org_id}, headers=tenant_headers)
        expect(summary.status_code == 200, "feedback summary failed")

        disconnect = client.post(
            f"{base}/api/v1/connectors/{conn}/disconnect",
            params={"org_id": args.org_id},
            headers=tenant_headers,
        )
        expect(disconnect.status_code == 200, "disconnect failed")
        expect(disconnect.json().get("status") == "disconnected", "disconnect status unexpected")

        print(json.dumps({
            "ok": True,
            "health": health.json(),
            "ready": ready.json(),
            "dashboard": dash,
            "memo": memo_payload,
            "feedback_summary": summary.json(),
            "disconnect": disconnect.json(),
        }, indent=2))

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"SMOKE FAILED: {exc}", file=sys.stderr)
        raise
