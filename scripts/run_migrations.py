#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from data_autopilot.db.session import SessionLocal, engine
from data_autopilot.services.migration_runner import MigrationRunner


def main() -> int:
    runner = MigrationRunner(engine)
    db = SessionLocal()
    try:
        summary = runner.run(db)
        payload = MigrationRunner.as_dict(summary)
        print(json.dumps(payload, indent=2))
        return 0 if payload["ok"] else 1
    finally:
        db.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"MIGRATION FAILED: {exc}", file=sys.stderr)
        raise
