"""Validate generated SQL against the live schema — catch hallucinated tables and columns."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


# Capture the table name immediately after FROM or JOIN.
# Intentionally stops at the first word — alias resolution is handled separately.
_TABLE_RE = re.compile(
    r"\b(?:FROM|JOIN)\s+([`\"\[]?[\w]+[`\"\]]?)",
    re.IGNORECASE,
)

# Explicit alias declarations only: <table> AS <alias>
# We avoid the ambiguous "table alias" form without AS to prevent eating keywords.
_ALIAS_AS_RE = re.compile(
    r"\b([\w]+)\s+AS\s+([a-zA-Z_]\w*)\b",
    re.IGNORECASE,
)

# Implicit alias: FROM/JOIN <table> <alias> where alias is a short identifier
# that is NOT a SQL keyword.
_SQL_KEYWORDS = frozenset(
    "ON WHERE AND OR NOT IN IS NULL AS JOIN INNER OUTER LEFT RIGHT CROSS "
    "FULL GROUP BY ORDER HAVING LIMIT OFFSET SELECT FROM UNION ALL DISTINCT "
    "CASE WHEN THEN ELSE END WITH".split()
)

# Qualified column references: alias.column or table.column
_QUAL_COL_RE = re.compile(r"\b(\w+)\.(\w+)\b")


@dataclass
class SchemaIssue:
    severity: str  # "error" | "warning"
    message: str
    suggestion: str | None = None

    def __str__(self) -> str:
        base = f"[{self.severity.upper()}] {self.message}"
        return f"{base} ({self.suggestion})" if self.suggestion else base


class SchemaValidator:
    """
    Parses a SQL string and verifies that every table and qualified column
    reference (e.g. u.name, orders.total) exists in the provided schema dict.

    Uses a simple fuzzy match (character-level similarity) to suggest
    corrections when something is wrong — much more useful than a raw DB error.
    """

    def validate(self, sql: str, schema: dict[str, Any]) -> list[SchemaIssue]:
        issues: list[SchemaIssue] = []
        table_names = set(schema.keys())
        lower_table_map = {t.lower(): t for t in table_names}  # lowercase → real name

        # Strip string literals so we don't false-positive on quoted values.
        clean_sql = re.sub(r"'[^']*'", "''", sql)

        # 1. Find all table references and build alias → real_table map.
        referenced_tables: list[str] = []
        alias_map: dict[str, str] = {}  # lowercase alias → real table name

        for match in _TABLE_RE.finditer(clean_sql):
            raw = match.group(1).strip("`\"[]")
            referenced_tables.append(raw)

        # Build alias map: explicit AS aliases first.
        for match in _ALIAS_AS_RE.finditer(clean_sql):
            tbl_raw = match.group(1).strip("`\"[]")
            alias = match.group(2).lower()
            real = lower_table_map.get(tbl_raw.lower())
            if real:
                alias_map[alias] = real

        # Implicit aliases: FROM/JOIN <table> <short_alias_not_a_keyword>
        _IMPLICIT_RE = re.compile(
            r"\b(?:FROM|JOIN)\s+([\w]+)\s+([a-zA-Z_]\w*)\b", re.IGNORECASE
        )
        for match in _IMPLICIT_RE.finditer(clean_sql):
            tbl_raw, alias = match.group(1), match.group(2)
            if alias.upper() in _SQL_KEYWORDS:
                continue
            real = lower_table_map.get(tbl_raw.lower())
            if real:
                alias_map[alias.lower()] = real

        # 2. Check each referenced table exists.
        for raw in referenced_tables:
            if raw.lower() not in lower_table_map:
                suggestion = _fuzzy_best(raw, table_names)
                issues.append(SchemaIssue(
                    severity="error",
                    message=f"Table '{raw}' not found in schema.",
                    suggestion=f"Did you mean '{suggestion}'?" if suggestion else None,
                ))

        # 3. Check qualified column references (alias.col or table.col).
        for qualifier, col in _QUAL_COL_RE.findall(clean_sql):
            qualifier_lc = qualifier.lower()

            # Resolve qualifier to a real table name.
            real_table = alias_map.get(qualifier_lc) or lower_table_map.get(qualifier_lc)
            if real_table is None:
                continue  # qualifier is not a known table/alias — skip (could be schema prefix, etc.)

            col_names = {c["name"] for c in schema[real_table]["columns"]}
            if col not in col_names:
                col_names_lc = {c.lower() for c in col_names}
                if col.lower() in col_names_lc:
                    continue  # case mismatch only, most DBs are case-insensitive on columns
                suggestion = _fuzzy_best(col, col_names)
                issues.append(SchemaIssue(
                    severity="error",
                    message=f"Column '{col}' not found on table '{real_table}'.",
                    suggestion=f"Did you mean '{suggestion}'?" if suggestion else None,
                ))

        return issues


# ------------------------------------------------------------------
# Fuzzy matching — no external deps, just character overlap ratio
# ------------------------------------------------------------------

def _similarity(a: str, b: str) -> float:
    """Jaccard similarity on bigrams.  Good enough for column/table name typos."""
    a, b = a.lower(), b.lower()
    if a == b:
        return 1.0
    if not a or not b:
        return 0.0
    bigrams_a = {a[i:i+2] for i in range(len(a) - 1)}
    bigrams_b = {b[i:i+2] for i in range(len(b) - 1)}
    if not bigrams_a or not bigrams_b:
        return 0.0
    return len(bigrams_a & bigrams_b) / len(bigrams_a | bigrams_b)


def _fuzzy_best(target: str, candidates: set[str], threshold: float = 0.2) -> str | None:
    scored = [(c, _similarity(target, c)) for c in candidates]
    best_name, best_score = max(scored, key=lambda x: x[1], default=(None, 0))
    return best_name if best_score >= threshold else None
