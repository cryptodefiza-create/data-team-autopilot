from __future__ import annotations

from typing import Any

from data_autopilot.services.mode1.models import (
    Interpretation,
    OutputFormat,
    Provenance,
    ProviderResult,
)


class ResponseFormatter:
    def format(
        self, result: ProviderResult, output: OutputFormat = OutputFormat.TABLE
    ) -> dict[str, Any]:
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

    def format_rich(
        self,
        records: list[dict[str, Any]],
        provenance: Provenance,
        interpretation: Interpretation | None = None,
        output_format: OutputFormat = OutputFormat.TABLE,
        truncated: bool = False,
        total_available: int = 0,
    ) -> dict[str, Any]:
        """Format with provenance footer and optional interpretation."""
        table = self._to_markdown_table(records)

        summary_parts = [f"Fetched {len(records)} records from {provenance.source}."]
        if truncated:
            summary_parts.append(f"(truncated from {total_available} total)")

        data: dict[str, Any] = {
            "table": table,
            "records": records,
            "total_available": total_available or len(records),
            "truncated": truncated,
            "provenance": {
                "source": provenance.source,
                "timestamp": provenance.timestamp.isoformat(),
                "chain": provenance.chain,
                "record_count": provenance.record_count,
                "truncated": provenance.truncated,
                "params": provenance.params,
                "filters": provenance.filters,
            },
            "provenance_footer": provenance.format_footer(),
        }

        warnings: list[str] = []
        if truncated:
            warnings.append("truncated")

        if interpretation:
            data["interpretation"] = {
                "text": interpretation.text,
                "stats": interpretation.stats,
                "disclaimer": interpretation.disclaimer,
            }

        return {
            "response_type": "blockchain_result",
            "summary": " ".join(summary_parts),
            "data": data,
            "warnings": warnings,
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
