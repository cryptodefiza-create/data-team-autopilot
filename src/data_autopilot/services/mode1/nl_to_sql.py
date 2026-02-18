from __future__ import annotations

import logging
from typing import Any

from data_autopilot.services.mode1.entity_aliases import EntityAliasManager
from data_autopilot.services.mode1.models import SchemaProfile, SQLQuery, ThinContract
from data_autopilot.services.mode1.sql_validator import SQLValidator

logger = logging.getLogger(__name__)

# Keyword → SQL pattern mapping for fallback (no LLM)
_KEYWORD_PATTERNS = {
    "count": "SELECT COUNT(*) FROM {table}",
    "how many": "SELECT COUNT(*) FROM {table}",
    "total": "SELECT SUM({value_col}) FROM {table}",
    "average": "SELECT AVG({value_col}) FROM {table}",
    "avg": "SELECT AVG({value_col}) FROM {table}",
    "top": "SELECT * FROM {table} ORDER BY {value_col} DESC LIMIT 10",
    "latest": "SELECT * FROM {table} ORDER BY {time_col} DESC LIMIT 10",
    "recent": "SELECT * FROM {table} ORDER BY {time_col} DESC LIMIT 10",
}


class NLToSQL:
    """Converts natural language questions to SQL queries.

    Uses LLM when available, falls back to keyword-based generation.
    """

    def __init__(
        self,
        alias_manager: EntityAliasManager | None = None,
        validator: SQLValidator | None = None,
    ) -> None:
        self._aliases = alias_manager or EntityAliasManager()
        self._validator = validator or SQLValidator()

    def generate(
        self,
        question: str,
        schema: SchemaProfile,
        org_id: str = "",
        contract: ThinContract | None = None,
    ) -> SQLQuery:
        """Generate SQL from a natural language question."""
        # Resolve table aliases
        table = self._resolve_table(question, schema, org_id)
        if table is None and schema.tables:
            table = schema.tables[0].name  # Default to first table

        if table is None:
            return SQLQuery(
                sql="", validated=False,
                error="No tables found in schema",
            )

        # Generate SQL via keyword fallback
        sql = self._keyword_generate(question, table, schema, contract)

        # Validate
        return self._validator.validate(sql, schema)

    def _resolve_table(
        self, question: str, schema: SchemaProfile, org_id: str
    ) -> str | None:
        """Resolve the target table from the question."""
        # Try alias manager first
        if org_id:
            resolved = self._aliases.get_table_for_query(org_id, question)
            if resolved:
                return resolved

        # Try matching table names directly
        text_lower = question.lower()
        for table in schema.tables:
            if table.name.lower() in text_lower:
                return table.name

        return None

    def _keyword_generate(
        self,
        question: str,
        table: str,
        schema: SchemaProfile,
        contract: ThinContract | None,
    ) -> str:
        """Generate SQL from keywords (fallback when LLM unavailable)."""
        text = question.lower()
        table_profile = next((t for t in schema.tables if t.name == table), None)

        value_col = self._find_value_column(table_profile)
        time_col = self._find_time_column(table_profile)

        # Apply contract adjustments
        revenue_expr = value_col
        if contract and value_col:
            revenue_expr = self._apply_contract(value_col, contract)

        # Match keyword patterns
        for keyword, pattern in _KEYWORD_PATTERNS.items():
            if keyword in text:
                sql = pattern.format(
                    table=table,
                    value_col=revenue_expr or "*",
                    time_col=time_col or "id",
                )

                # Add GROUP BY if aggregation with "by"
                if "by" in text and keyword in ("total", "average", "avg"):
                    group_col = self._detect_group_column(text, table_profile)
                    if group_col:
                        sql = (
                            f"SELECT {group_col}, {keyword.upper() if keyword != 'total' else 'SUM'}"
                            f"({revenue_expr}) FROM {table} GROUP BY {group_col}"
                        )

                # Add time filter for "last week/month"
                if time_col:
                    time_filter = self._detect_time_filter(text, time_col)
                    if time_filter:
                        if "WHERE" in sql.upper():
                            sql = sql.replace("WHERE", f"WHERE {time_filter} AND")
                        else:
                            # Insert WHERE before ORDER BY or GROUP BY or at end
                            for clause in ("ORDER BY", "GROUP BY", "LIMIT"):
                                if clause in sql.upper():
                                    idx = sql.upper().index(clause)
                                    sql = f"{sql[:idx]}WHERE {time_filter} {sql[idx:]}"
                                    break
                            else:
                                sql = f"{sql} WHERE {time_filter}"

                return sql

        # Default: SELECT * with LIMIT
        return f"SELECT * FROM {table}"

    @staticmethod
    def _find_value_column(table_profile: Any) -> str | None:
        if table_profile is None:
            return None
        value_names = {"amount", "revenue", "total", "price", "value", "total_price", "balance"}
        for col in table_profile.columns:
            if col.name.lower() in value_names:
                return col.name
        # Fallback to first numeric-looking column
        numeric_types = {"integer", "int", "bigint", "float", "double", "decimal", "numeric"}
        for col in table_profile.columns:
            if col.data_type.lower() in numeric_types:
                return col.name
        return None

    @staticmethod
    def _find_time_column(table_profile: Any) -> str | None:
        if table_profile is None:
            return None
        if table_profile.detected_time_columns:
            return table_profile.detected_time_columns[0]
        return None

    @staticmethod
    def _apply_contract(value_col: str, contract: ThinContract) -> str:
        if contract.revenue_definition == "net_after_refunds":
            return f"({value_col} - COALESCE(refund_amount, 0))"
        elif contract.revenue_definition == "net_after_refunds_and_tax":
            return f"({value_col} - COALESCE(refund_amount, 0) - COALESCE(tax, 0))"
        return value_col

    @staticmethod
    def _detect_group_column(text: str, table_profile: Any) -> str | None:
        """Detect what column to GROUP BY from the question."""
        if table_profile is None:
            return None
        group_hints = {
            "category": "category",
            "product": "product_type",
            "customer": "customer_id",
            "month": "month",
            "week": "week",
            "day": "day",
            "status": "status",
        }
        for hint, col_name in group_hints.items():
            if hint in text:
                # Check if the column exists
                for col in table_profile.columns:
                    if col.name.lower() == col_name:
                        return col.name
        return None

    @staticmethod
    def _detect_time_filter(text: str, time_col: str) -> str | None:
        """Detect time filters like 'last week', 'last month'."""
        if "last week" in text:
            return f"{time_col} >= NOW() - INTERVAL '7 days'"
        if "last month" in text:
            return f"{time_col} >= NOW() - INTERVAL '30 days'"
        if "last year" in text:
            return f"{time_col} >= NOW() - INTERVAL '365 days'"
        if "yesterday" in text:
            return f"{time_col} >= NOW() - INTERVAL '1 day'"
        return None
