from __future__ import annotations

import logging
import re

from data_autopilot.services.mode1.models import SQLQuery, SchemaProfile

logger = logging.getLogger(__name__)

_FORBIDDEN_STATEMENTS = {
    "insert", "update", "delete", "drop", "create", "alter",
    "truncate", "grant", "revoke", "exec", "execute",
}

_DEFAULT_LIMIT = 10000


class UnsafeSQLError(ValueError):
    pass


class InvalidTableError(ValueError):
    pass


class SQLValidator:
    """Validates generated SQL for safety before execution.

    Checks:
    1. SELECT-only (no DDL/DML)
    2. All referenced tables exist in schema
    3. Adds LIMIT if missing
    """

    def validate(self, sql: str, schema: SchemaProfile) -> SQLQuery:
        """Validate SQL and return a SQLQuery with validated=True or error."""
        sql = sql.strip().rstrip(";")

        try:
            self._check_statement_type(sql)
            self._check_tables(sql, schema)
            sql = self._ensure_limit(sql)

            return SQLQuery(sql=sql, validated=True)

        except (UnsafeSQLError, InvalidTableError) as exc:
            logger.warning("SQL validation failed: %s", exc)
            return SQLQuery(sql=sql, validated=False, error=str(exc))

    def _check_statement_type(self, sql: str) -> None:
        """Ensure the SQL is a SELECT statement only."""
        # Get the first keyword
        first_word = sql.split()[0].lower() if sql.split() else ""
        if first_word != "select" and first_word != "with":
            raise UnsafeSQLError(
                f"Only SELECT queries are allowed. Got: {first_word.upper()}"
            )

        # Check for embedded DML/DDL anywhere in the query
        sql_lower = sql.lower()
        for forbidden in _FORBIDDEN_STATEMENTS:
            # Match as whole word to avoid false positives like "created_at"
            pattern = rf"\b{forbidden}\b"
            # Skip if it's in a string literal or column name context
            if forbidden in ("create", "update", "delete", "alter"):
                # These could appear as column suffixes like "created_at", "updated_at"
                # Only flag if they appear at the start of a clause
                if re.search(rf";\s*{forbidden}\b", sql_lower):
                    raise UnsafeSQLError(f"Statement contains forbidden keyword: {forbidden.upper()}")
                if sql_lower.strip().startswith(forbidden):
                    raise UnsafeSQLError(f"Statement starts with forbidden keyword: {forbidden.upper()}")
            elif re.search(pattern, sql_lower):
                # For truly dangerous keywords like DROP, TRUNCATE, GRANT
                if forbidden in ("drop", "truncate", "grant", "revoke", "exec", "execute"):
                    raise UnsafeSQLError(f"Statement contains forbidden keyword: {forbidden.upper()}")

    def _check_tables(self, sql: str, schema: SchemaProfile) -> None:
        """Ensure all referenced tables exist in the schema."""
        if not schema.tables:
            return  # No schema to validate against

        referenced = self._extract_table_names(sql)
        valid_tables = {t.name.lower() for t in schema.tables}

        for table in referenced:
            if table.lower() not in valid_tables:
                raise InvalidTableError(
                    f"Table '{table}' not found in schema. "
                    f"Available tables: {', '.join(sorted(valid_tables))}"
                )

    def _ensure_limit(self, sql: str) -> str:
        """Add LIMIT if not present."""
        if re.search(r"\bLIMIT\b", sql, re.IGNORECASE):
            return sql
        return f"{sql} LIMIT {_DEFAULT_LIMIT}"

    @staticmethod
    def _extract_table_names(sql: str) -> list[str]:
        """Extract table names from FROM and JOIN clauses."""
        tables: list[str] = []
        sql_lower = sql.lower()

        # Match FROM <table> and JOIN <table>
        patterns = [
            r'\bfrom\s+(\w+)',
            r'\bjoin\s+(\w+)',
        ]
        for pattern in patterns:
            matches = re.findall(pattern, sql_lower)
            tables.extend(matches)

        # Filter out SQL keywords that might be picked up
        sql_keywords = {"select", "where", "and", "or", "on", "as", "in", "not", "null", "lateral"}
        return [t for t in tables if t not in sql_keywords]
