#!/usr/bin/env python3
"""In-process load test for Data Team Autopilot.

Runs concurrent requests against the FastAPI app using TestClient (no network
sockets). Reports p50/p95/p99 latency and error rate. Exit code 1 if thresholds
are not met.

Usage:
    python scripts/load_test_inprocess.py --requests 200 --concurrency 10
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    idx = int((len(s) - 1) * p)
    return s[idx]


def main() -> int:
    parser = argparse.ArgumentParser(description="In-process load test (no network sockets)")
    parser.add_argument("--org-id", default="org_load_inproc")
    parser.add_argument("--requests", type=int, default=200)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument(
        "--p95-threshold-ms", type=float, default=5000,
        help="p95 latency threshold in ms (default: 5000)",
    )
    parser.add_argument(
        "--error-rate-threshold", type=float, default=0.01,
        help="Max error rate as fraction (default: 0.01 = 1%%)",
    )
    args = parser.parse_args()

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    src_dir = os.path.join(root, "src")
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)

    from fastapi.testclient import TestClient
    from data_autopilot.main import app

    client = TestClient(app)
    headers = {"X-Tenant-Id": args.org_id, "X-User-Role": "member"}

    # Mix of endpoint types for realistic load profile
    endpoints = [
        ("GET", "/health", None, None),
        ("GET", "/ready", None, None),
        ("GET", "/api/v1/llm/status", None, None),
        ("POST", "/api/v1/workflows/profile", None, {"org_id": args.org_id}),
        ("POST", "/api/v1/chat/run", {
            "org_id": args.org_id,
            "user_id": "user_load",
            "session_id": "load-test",
            "message": "show me DAU",
        }, None),
    ]

    latencies: list[float] = []
    errors = 0

    def fire(idx: int) -> tuple[float, bool]:
        method, path, body, params = endpoints[idx % len(endpoints)]
        t0 = time.perf_counter()
        try:
            if method == "GET":
                r = client.get(path, headers=headers)
            else:
                r = client.post(path, json=body, headers=headers, params=params)
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            return elapsed_ms, r.status_code >= 500
        except Exception:
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            return elapsed_ms, True

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=max(1, args.concurrency)) as pool:
        futures = {pool.submit(fire, i): i for i in range(max(1, args.requests))}
        for fut in as_completed(futures):
            elapsed_ms, is_error = fut.result()
            latencies.append(elapsed_ms)
            if is_error:
                errors += 1
    wall_time = time.perf_counter() - start

    total = max(1, args.requests)
    error_rate = errors / total

    p50 = round(percentile(latencies, 0.50), 2) if latencies else 0
    p95 = round(percentile(latencies, 0.95), 2) if latencies else 0
    p99 = round(percentile(latencies, 0.99), 2) if latencies else 0

    result = {
        "total_requests": total,
        "concurrency": args.concurrency,
        "errors": errors,
        "error_rate": round(error_rate, 4),
        "duration_sec": round(wall_time, 3),
        "throughput_rps": round(total / wall_time, 2) if wall_time > 0 else 0.0,
        "p50_ms": p50,
        "p95_ms": p95,
        "p99_ms": p99,
        "avg_ms": round(sum(latencies) / total, 2) if latencies else 0,
    }
    print(json.dumps(result, indent=2))

    passed = True
    if p95 > args.p95_threshold_ms:
        print(f"FAIL: p95 {p95}ms exceeds threshold {args.p95_threshold_ms}ms", file=sys.stderr)
        passed = False
    if error_rate > args.error_rate_threshold:
        print(
            f"FAIL: error rate {error_rate:.2%} exceeds threshold "
            f"{args.error_rate_threshold:.2%}",
            file=sys.stderr,
        )
        passed = False

    if passed:
        print("PASS: All load test thresholds met.")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
