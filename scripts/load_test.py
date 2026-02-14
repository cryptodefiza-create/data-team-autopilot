#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import statistics
import time

import httpx


async def hit(client: httpx.AsyncClient, method: str, url: str, **kwargs) -> float:
    start = time.perf_counter()
    r = await client.request(method, url, **kwargs)
    r.raise_for_status()
    return time.perf_counter() - start


async def run_load(base_url: str, org_id: str, duration: int, rps: int) -> dict:
    latencies: list[float] = []
    errors = 0
    total = 0
    interval = 1.0 / max(1, rps)

    async with httpx.AsyncClient(timeout=30) as client:
        headers = {"X-Tenant-Id": org_id, "X-User-Role": "member"}
        end_time = time.time() + duration
        while time.time() < end_time:
            total += 1
            try:
                l = await hit(
                    client,
                    "POST",
                    f"{base_url}/api/v1/workflows/profile",
                    params={"org_id": org_id},
                    headers=headers,
                )
                latencies.append(l)
            except Exception:
                errors += 1
            await asyncio.sleep(interval)

    if not latencies:
        return {"total": total, "errors": errors, "p50_ms": None, "p95_ms": None, "p99_ms": None}

    s = sorted(latencies)
    def pct(p: float) -> float:
        idx = int(len(s) * p) - 1
        idx = max(0, min(idx, len(s) - 1))
        return s[idx] * 1000

    return {
        "total": total,
        "errors": errors,
        "error_rate": (errors / total) if total else 0,
        "p50_ms": round(pct(0.5), 2),
        "p95_ms": round(pct(0.95), 2),
        "p99_ms": round(pct(0.99), 2),
        "avg_ms": round(statistics.mean(latencies) * 1000, 2),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Simple load test for Data Team Autopilot")
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--org-id", default="org_load")
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--rps", type=int, default=2)
    args = parser.parse_args()

    result = asyncio.run(run_load(args.base_url.rstrip("/"), args.org_id, args.duration, args.rps))
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
