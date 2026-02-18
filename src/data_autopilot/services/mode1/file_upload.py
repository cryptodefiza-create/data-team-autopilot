from __future__ import annotations

import csv
import io
import json
import logging
from typing import Any

from data_autopilot.services.mode1.models import RawDataset

logger = logging.getLogger(__name__)

_SUPPORTED_TYPES = {
    "text/csv",
    "application/csv",
    "application/json",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}


class UnsupportedFileType(ValueError):
    pass


class FileUploadHandler:
    def process(self, content: bytes, content_type: str, filename: str = "") -> RawDataset:
        if content_type not in _SUPPORTED_TYPES:
            # Try to infer from filename
            if filename.endswith(".csv"):
                content_type = "text/csv"
            elif filename.endswith(".json"):
                content_type = "application/json"
            elif filename.endswith(".xlsx"):
                content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            else:
                raise UnsupportedFileType(f"Unsupported file type: {content_type}")

        if content_type in ("text/csv", "application/csv"):
            data = self._parse_csv(content)
        elif content_type == "application/json":
            data = self._parse_json(content)
        else:
            data = self._parse_xlsx(content)

        return RawDataset(
            records=data,
            source="file_upload",
            record_count=len(data),
        )

    @staticmethod
    def _parse_csv(content: bytes) -> list[dict[str, Any]]:
        text = content.decode("utf-8-sig")
        # Skip comment lines (provenance headers)
        lines = [line for line in text.splitlines() if not line.startswith("#")]
        reader = csv.DictReader(io.StringIO("\n".join(lines)))
        return [dict(row) for row in reader]

    @staticmethod
    def _parse_json(content: bytes) -> list[dict[str, Any]]:
        data = json.loads(content)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            # Try common wrapper keys
            for key in ("data", "records", "results", "items"):
                if isinstance(data.get(key), list):
                    return data[key]
            return [data]
        return []

    @staticmethod
    def _parse_xlsx(content: bytes) -> list[dict[str, Any]]:
        import openpyxl

        wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        if len(rows) < 2:
            return []
        headers = [str(h) if h is not None else f"col_{i}" for i, h in enumerate(rows[0])]
        return [
            {headers[i]: cell for i, cell in enumerate(row) if i < len(headers)}
            for row in rows[1:]
        ]
