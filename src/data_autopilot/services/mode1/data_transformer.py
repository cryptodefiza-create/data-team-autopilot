from __future__ import annotations

from typing import Any


class DataTransformer:
    def filter(self, data: list[dict[str, Any]], filters: dict[str, Any]) -> list[dict[str, Any]]:
        """Apply key-value filters. Supports _min/_max suffixes for numeric ranges."""
        result = data
        for key, value in filters.items():
            if key.endswith("_min"):
                field = key[:-4]
                threshold = float(value)
                result = [
                    r for r in result
                    if (v := self._to_float(r.get(field))) is not None and v >= threshold
                ]
            elif key.endswith("_max"):
                field = key[:-4]
                threshold = float(value)
                result = [
                    r for r in result
                    if (v := self._to_float(r.get(field))) is not None and v <= threshold
                ]
            else:
                result = [r for r in result if str(r.get(key, "")).lower() == str(value).lower()]
        return result

    def sort(
        self, data: list[dict[str, Any]], sort_key: str, descending: bool = True
    ) -> list[dict[str, Any]]:
        return sorted(
            data,
            key=lambda r: self._to_float(r.get(sort_key)) or 0,
            reverse=descending,
        )

    def aggregate(
        self,
        data: list[dict[str, Any]],
        group_by: str,
        metrics: list[str],
    ) -> list[dict[str, Any]]:
        """GROUP BY equivalent: groups by a key, sums/counts metrics."""
        groups: dict[str, dict[str, Any]] = {}
        for row in data:
            key = str(row.get(group_by, "unknown"))
            if key not in groups:
                groups[key] = {group_by: key, "count": 0}
                for m in metrics:
                    groups[key][f"{m}_sum"] = 0.0
            groups[key]["count"] += 1
            for m in metrics:
                val = self._to_float(row.get(m))
                if val is not None:
                    groups[key][f"{m}_sum"] += val
        return list(groups.values())

    def add_computed_columns(
        self,
        data: list[dict[str, Any]],
        computations: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Add computed columns. Each computation: {name, field, operation, operand}.
        Supported operations: pct_of_total, multiply, divide.
        """
        for comp in computations:
            name = comp["name"]
            field = comp["field"]
            operation = comp["operation"]
            operand = comp.get("operand", 1)

            if operation == "pct_of_total":
                total = sum(
                    self._to_float(r.get(field)) or 0 for r in data
                )
                for row in data:
                    val = self._to_float(row.get(field)) or 0
                    row[name] = round(val / total * 100, 4) if total else 0
            elif operation == "multiply":
                for row in data:
                    val = self._to_float(row.get(field)) or 0
                    row[name] = val * float(operand)
            elif operation == "divide":
                for row in data:
                    val = self._to_float(row.get(field)) or 0
                    row[name] = val / float(operand) if float(operand) != 0 else 0

        return data

    @staticmethod
    def _to_float(val: Any) -> float | None:
        if val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None
