#!/usr/bin/env python3
from __future__ import annotations

import importlib.metadata as metadata
import argparse
import re
from pathlib import Path

try:
    import tomllib  # type: ignore[attr-defined]
except ModuleNotFoundError:  # pragma: no cover - py<3.11 fallback
    import tomli as tomllib  # type: ignore[no-redef]


REQ_RE = re.compile(r"^\s*([A-Za-z0-9_.\-]+)==([^\s;]+)")


def normalize(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def parse_pinned(req: str) -> tuple[str, str] | None:
    match = REQ_RE.match(req)
    if not match:
        return None
    return match.group(1), match.group(2)


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate pinned dependencies are installed.")
    parser.add_argument(
        "--include-live",
        action="store_true",
        help="also validate [project.optional-dependencies.live]",
    )
    args = parser.parse_args()

    pyproject = Path("pyproject.toml")
    data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    project = data.get("project", {})
    optional = project.get("optional-dependencies", {})

    reqs: list[str] = list(project.get("dependencies", []))
    if args.include_live:
        reqs.extend(optional.get("live", []))

    errors: list[str] = []
    for req in reqs:
        parsed = parse_pinned(req)
        if not parsed:
            errors.append(f"dependency is not exact-pinned: {req}")
            continue
        pkg, expected = parsed
        try:
            installed = metadata.version(pkg)
        except metadata.PackageNotFoundError:
            errors.append(f"dependency not installed: {pkg}=={expected}")
            continue
        if installed != expected:
            errors.append(
                f"version mismatch for {pkg}: installed={installed} expected={expected}"
            )

    if errors:
        print("FAIL: dependency integrity check failed")
        for err in errors:
            print(f"- {err}")
        return 1

    print("PASS: dependency integrity check passed (exact pins + installed versions)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
