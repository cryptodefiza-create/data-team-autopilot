from __future__ import annotations

import re
from dataclasses import dataclass, field

try:
    import sqlglot
    from sqlglot import exp
except Exception:  # pragma: no cover
    sqlglot = None
    exp = None

BLOCKED = {"Create", "Alter", "Drop", "TruncateTable", "Insert", "Update", "Delete", "Merge"}
DANGEROUS_COMMENT_PATTERN = re.compile(r"(--|/\*).*?\b(create|alter|drop|truncate|insert|update|delete|merge)\b", re.IGNORECASE | re.DOTALL)


@dataclass
class SqlSafetyDecision:
    allowed: bool
    rewritten_sql: str | None = None
    reasons: list[str] = field(default_factory=list)


class SqlSafetyEngine:
    def __init__(self, default_limit: int = 10_000, max_join_depth: int = 5, max_subquery_depth: int = 3) -> None:
        self.default_limit = default_limit
        self.max_join_depth = max_join_depth
        self.max_subquery_depth = max_subquery_depth
        self.partition_columns = {
            "analytics.events": "created_at",
            "analytics.orders": "created_at",
            "analytics.users": "created_at",
        }

    def evaluate(self, sql: str) -> SqlSafetyDecision:
        if DANGEROUS_COMMENT_PATTERN.search(sql):
            return SqlSafetyDecision(allowed=False, reasons=["Dangerous SQL found in comments"])

        if sqlglot is None:
            return self._evaluate_fallback(sql)

        try:
            parsed = sqlglot.parse(sql)
        except Exception:
            return SqlSafetyDecision(allowed=False, reasons=["Invalid SQL"]) 

        if len(parsed) != 1:
            return SqlSafetyDecision(allowed=False, reasons=["Multi-statement SQL is blocked"])

        root = parsed[0]
        for node in root.walk():
            if node.__class__.__name__ in BLOCKED:
                return SqlSafetyDecision(allowed=False, reasons=[f"Blocked operation: {node.__class__.__name__}"])

        join_depth = sum(1 for node in root.walk() if isinstance(node, exp.Join))
        if join_depth > self.max_join_depth:
            return SqlSafetyDecision(allowed=False, reasons=[f"Join depth exceeds max ({self.max_join_depth})"])

        if self._max_subquery_depth(root) > self.max_subquery_depth:
            return SqlSafetyDecision(allowed=False, reasons=[f"Subquery nesting exceeds max ({self.max_subquery_depth})"])

        if not isinstance(root, exp.Select):
            select_stmt = root.find(exp.Select)
        else:
            select_stmt = root

        if select_stmt is None:
            return SqlSafetyDecision(allowed=False, reasons=["Only SELECT queries are allowed"])

        table_names = self._table_names(root)
        for table in table_names:
            normalized = table.strip()
            partition_col = self.partition_columns.get(normalized)
            if partition_col is None:
                for configured, col in self.partition_columns.items():
                    if normalized.endswith(configured):
                        partition_col = col
                        break
            if partition_col and not self._has_time_filter(select_stmt, partition_col):
                try:
                    filter_expr = sqlglot.parse_one(f"{partition_col} >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)")
                except Exception:
                    filter_expr = None
                if filter_expr is None:
                    return SqlSafetyDecision(allowed=False, reasons=[f"Missing required partition filter on {partition_col}"])
                rewritten_select = select_stmt.where(filter_expr)
                rewritten_sql = rewritten_select.sql()
                reasons = ["Partition filter auto-added"]
                if " LIMIT " not in rewritten_sql.upper():
                    rewritten_sql = f"{rewritten_sql} LIMIT {self.default_limit}"
                    reasons.append("LIMIT auto-added")
                return SqlSafetyDecision(allowed=True, rewritten_sql=rewritten_sql, reasons=reasons)

        has_aggregate = any(isinstance(node, exp.AggFunc) for node in root.walk())
        has_limit = select_stmt.args.get("limit") is not None

        if not has_aggregate and not has_limit:
            rewritten = select_stmt.limit(self.default_limit).sql()
            return SqlSafetyDecision(allowed=True, rewritten_sql=rewritten, reasons=["LIMIT auto-added"])

        return SqlSafetyDecision(allowed=True, rewritten_sql=sql)

    def _evaluate_fallback(self, sql: str) -> SqlSafetyDecision:
        stripped = sql.strip()
        if DANGEROUS_COMMENT_PATTERN.search(stripped):
            return SqlSafetyDecision(allowed=False, reasons=["Dangerous SQL found in comments"])
        if ";" in stripped[:-1]:
            return SqlSafetyDecision(allowed=False, reasons=["Multi-statement SQL is blocked"])

        upper_sql = stripped.upper()
        blocked_keywords = ("CREATE ", "ALTER ", "DROP ", "TRUNCATE ", "INSERT ", "UPDATE ", "DELETE ", "MERGE ")
        if upper_sql.startswith(blocked_keywords):
            return SqlSafetyDecision(allowed=False, reasons=["Blocked non-SELECT operation"])

        if not upper_sql.startswith("SELECT "):
            return SqlSafetyDecision(allowed=False, reasons=["Only SELECT queries are allowed"])

        for table, partition_col in self.partition_columns.items():
            has_table = f" {table.upper()} " in f" {upper_sql} "
            where_match = re.search(r"\bWHERE\b(.+)", upper_sql)
            has_partition_in_where = bool(where_match and partition_col.upper() in where_match.group(1))
            if has_table and not has_partition_in_where:
                if " WHERE " in upper_sql:
                    rewritten = f"{stripped} AND {partition_col} >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)"
                else:
                    rewritten = f"{stripped} WHERE {partition_col} >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)"
                if " LIMIT " not in upper_sql:
                    rewritten = f"{rewritten} LIMIT {self.default_limit}"
                return SqlSafetyDecision(allowed=True, rewritten_sql=rewritten, reasons=["Partition filter auto-added"])

        join_count = upper_sql.count(" JOIN ")
        if join_count > self.max_join_depth:
            return SqlSafetyDecision(allowed=False, reasons=[f"Join depth exceeds max ({self.max_join_depth})"])

        subquery_count = upper_sql.count("(SELECT")
        if subquery_count > self.max_subquery_depth:
            return SqlSafetyDecision(allowed=False, reasons=[f"Subquery nesting exceeds max ({self.max_subquery_depth})"])

        has_aggregate = bool(re.search(r"\b(COUNT|SUM|AVG|MIN|MAX)\s*\(", upper_sql))
        has_limit = bool(re.search(r"\bLIMIT\s+\d+", upper_sql))
        if not has_aggregate and not has_limit:
            return SqlSafetyDecision(
                allowed=True,
                rewritten_sql=f"{stripped} LIMIT {self.default_limit}",
                reasons=["LIMIT auto-added"],
            )
        return SqlSafetyDecision(allowed=True, rewritten_sql=stripped)

    def _max_subquery_depth(self, root) -> int:
        if exp is None:
            return 0

        def walk(node, depth: int) -> int:
            max_depth = depth
            for child in node.iter_expressions():
                if isinstance(child, exp.Subquery):
                    max_depth = max(max_depth, walk(child, depth + 1))
                else:
                    max_depth = max(max_depth, walk(child, depth))
            return max_depth

        return walk(root, 0)

    def _table_names(self, root) -> list[str]:
        if exp is None:
            return []
        names: list[str] = []
        for node in root.walk():
            if isinstance(node, exp.Table):
                names.append(node.sql().lower().replace("`", "").replace('"', ""))
        return names

    def _has_time_filter(self, select_stmt, column_name: str) -> bool:
        if exp is None:
            return False
        where_expr = select_stmt.args.get("where")
        if where_expr is None:
            return False
        for node in where_expr.walk():
            if isinstance(node, exp.Column) and node.name.lower() == column_name.lower():
                return True
        return False
