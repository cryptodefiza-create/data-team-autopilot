from __future__ import annotations

from typing import Any

from data_autopilot.services.mode1.models import OutputFormat, ProviderResult


class ResponseFormatter:
    def format(self, result: ProviderResult, output: OutputFormat = OutputFormat.TABLE) -> dict[str, Any]:
        if result.error:
            return {
                "response_type": "error",
                "summary": f"Provider error: {result.error}",
                "data": {"provider": result.provider, "method": result.method},
                "warnings": ["provider_error"],
            }

        if output == OutputFormat.RAW:
            return {
                "response_type": "blockchain_result",
                "summary": f"Fetched {len(result.records)} records from {result.provider}.",
                "data": {
                    "records": result.records,
                    "total_available": result.total_available,
                    "truncated": result.truncated,
                },
                "warnings": ["truncated"] if result.truncated else [],
            }

        table = self._to_markdown_table(result.records)
        summary_parts = [f"Fetched {len(result.records)} records from {result.provider}."]
        if result.truncated:
            summary_parts.append(f"(truncated from {result.total_available} total)")

        return {
            "response_type": "blockchain_result",
            "summary": " ".join(summary_parts),
            "data": {
                "table": table,
                "records": result.records,
                "total_available": result.total_available,
                "truncated": result.truncated,
            },
            "warnings": ["truncated"] if result.truncated else [],
        }

    @staticmethod
    def _to_markdown_table(records: list[dict[str, Any]]) -> str:
        if not records:
            return "_No records_"
        headers = list(records[0].keys())
        lines = ["| " + " | ".join(str(h) for h in headers) + " |"]
        lines.append("| " + " | ".join("---" for _ in headers) + " |")
        for row in records[:50]:  # cap table rows for readability
            lines.append("| " + " | ".join(str(row.get(h, "")) for h in headers) + " |")
        if len(records) > 50:
            lines.append(f"_... and {len(records) - 50} more rows_")
        return "\n".join(lines)
