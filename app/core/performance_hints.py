"""SQL query performance analysis.

Inspects the executed SQL and the database schema to identify missing
indexes on WHERE-filter and JOIN columns — the two most common sources
of slow queries in analytics workloads.

Returns plain-English hints such as:
    "Consider adding an index on users.created_at (used in WHERE filter)"
    "Consider adding an index on orders.user_id (used in JOIN condition)"
"""
from __future__ import annotations

import re
import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class PerformanceHint:
    table: str
    column: str
    reason: str    # e.g. "used in WHERE filter"

    def __str__(self) -> str:
        return (
            f"Consider adding an index on {self.table}.{self.column} "
            f"({self.reason})"
        )


def analyze_performance(
    sql: str,
    schema: dict[str, Any],
) -> list[PerformanceHint]:
    """Return performance hints for the given SQL against the schema.

    Args:
        sql:    Executed SQL string.
        schema: Schema dict from SchemaLoader — keys are table names,
                values have 'columns', 'primary_keys', 'foreign_keys'.
    """
    hints: list[PerformanceHint] = []
    table_names_lower = {t.lower(): t for t in schema}

    if not table_names_lower:
        return hints

    # ── Columns already indexed (PKs + FK constrained columns) ─────────
    indexed_cols: set[tuple[str, str]] = set()
    for table, info in schema.items():
        for pk in info.get("primary_keys", []):
            indexed_cols.add((table.lower(), pk.lower()))
        for fk in info.get("foreign_keys", []):
            for col in fk.get("constrained_columns", []):
                indexed_cols.add((table.lower(), col.lower()))

    seen: set[tuple[str, str]] = set()

    def _add(table: str, column: str, reason: str) -> None:
        key = (table.lower(), column.lower())
        if key in seen or key in indexed_cols:
            return
        # Verify column exists in schema
        orig_table = table_names_lower.get(table.lower())
        if not orig_table:
            return
        col_names = [
            c["name"].lower() for c in schema[orig_table].get("columns", [])
        ]
        if column.lower() not in col_names:
            return
        seen.add(key)
        hints.append(PerformanceHint(table=orig_table, column=column, reason=reason))

    # ── WHERE clause ────────────────────────────────────────────────────
    where_m = re.search(
        r"\bWHERE\b(.+?)(?:\bGROUP\b|\bORDER\b|\bHAVING\b|\bLIMIT\b|$)",
        sql, re.IGNORECASE | re.DOTALL,
    )
    if where_m:
        for tbl, col in re.findall(r"\b(\w+)\.(\w+)\b", where_m.group(1)):
            if tbl.lower() in table_names_lower:
                _add(tbl, col, "used in WHERE filter")

    # ── JOIN … ON conditions ─────────────────────────────────────────────
    for join_m in re.finditer(
        r"\bJOIN\b.+?\bON\b\s*(.+?)(?:\bJOIN\b|\bWHERE\b|\bGROUP\b|\bORDER\b|\bLIMIT\b|$)",
        sql, re.IGNORECASE | re.DOTALL,
    ):
        for tbl, col in re.findall(r"\b(\w+)\.(\w+)\b", join_m.group(1)):
            if tbl.lower() in table_names_lower:
                _add(tbl, col, "used in JOIN condition")

    # ── ORDER BY columns ────────────────────────────────────────────────
    order_m = re.search(
        r"\bORDER\s+BY\b\s*(.+?)(?:\bLIMIT\b|$)",
        sql, re.IGNORECASE | re.DOTALL,
    )
    if order_m:
        for tbl, col in re.findall(r"\b(\w+)\.(\w+)\b", order_m.group(1)):
            if tbl.lower() in table_names_lower:
                _add(tbl, col, "used in ORDER BY")

    return hints
