#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import statistics
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
    args = parser.parse_args()

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    src_dir = os.path.join(root, "src")
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)

    from fastapi.testclient import TestClient
    from data_autopilot.main import app

    client = TestClient(app)
    headers = {"X-Tenant-Id": args.org_id, "X-User-Role": "member"}

    latencies: list[float] = []
    errors = 0

    def one_call() -> float:
        t0 = time.perf_counter()
        r = client.post("/api/v1/workflows/profile", params={"org_id": args.org_id}, headers=headers)
        if r.status_code != 200:
            raise RuntimeError(f"status={r.status_code}")
        return (time.perf_counter() - t0) * 1000.0

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=max(1, args.concurrency)) as ex:
        futures = [ex.submit(one_call) for _ in range(max(1, args.requests))]
        for fut in as_completed(futures):
            try:
                latencies.append(fut.result())
            except Exception:
                errors += 1
    elapsed = time.perf_counter() - start

    total = max(1, args.requests)
    result = {
        "total_requests": total,
        "errors": errors,
        "error_rate": round(errors / total, 4),
        "duration_sec": round(elapsed, 3),
        "throughput_rps": round(total / elapsed, 2) if elapsed > 0 else 0.0,
        "p50_ms": round(percentile(latencies, 0.50), 2) if latencies else None,
        "p95_ms": round(percentile(latencies, 0.95), 2) if latencies else None,
        "p99_ms": round(percentile(latencies, 0.99), 2) if latencies else None,
        "avg_ms": round(statistics.mean(latencies), 2) if latencies else None,
    }
    print(result)
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
