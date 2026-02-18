from __future__ import annotations

import csv
import io
from typing import Any

from data_autopilot.services.mode1.models import Provenance


class CSVGenerator:
    def generate(self, data: list[dict[str, Any]], metadata: Provenance) -> str:
        buf = io.StringIO()

        # Provenance comment header
        buf.write(f"# Source: {metadata.source}\n")
        buf.write(f"# Queried: {metadata.timestamp.isoformat()}\n")
        buf.write(f"# Chain: {metadata.chain or ''}\n")
        buf.write(f"# Records: {metadata.record_count}\n")
        buf.write(f"# Truncated: {metadata.truncated}\n")

        if data:
            headers = list(data[0].keys())
            writer = csv.DictWriter(buf, fieldnames=headers)
            writer.writeheader()
            writer.writerows(data)

        return buf.getvalue()
