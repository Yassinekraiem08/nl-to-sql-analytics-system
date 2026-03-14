"""Validate that a SQL string is safe to execute (SELECT only, no DDL/DML)."""
from __future__ import annotations

import re

import sqlparse
from sqlparse.sql import Statement
from sqlparse.tokens import Keyword, DDL, DML


_FORBIDDEN_RE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|CREATE|ALTER|REPLACE|MERGE|EXEC|EXECUTE|GRANT|REVOKE)\b",
    re.IGNORECASE,
)


class SQLValidationError(ValueError):
    pass


class SQLValidator:
    def validate(self, sql: str) -> None:
        """Raise *SQLValidationError* if *sql* is not a safe SELECT statement."""
        if not sql or not sql.strip():
            raise SQLValidationError("Empty SQL string.")

        # 1. Regex safety scan
        match = _FORBIDDEN_RE.search(sql)
        if match:
            raise SQLValidationError(
                f"Forbidden keyword '{match.group()}' found in SQL."
            )

        # 2. Parse and enforce single statement
        statements = [s for s in sqlparse.parse(sql) if s.get_type() is not None]
        if len(statements) == 0:
            raise SQLValidationError("Could not parse SQL statement.")
        if len(statements) > 1:
            raise SQLValidationError("Only a single SQL statement is allowed.")

        stmt: Statement = statements[0]

        # 3. Must be a SELECT
        stmt_type = stmt.get_type()
        if stmt_type != "SELECT":
            raise SQLValidationError(
                f"Only SELECT statements are allowed. Got: {stmt_type}"
            )

        # 4. Check token-level DDL/DML tokens (belt-and-suspenders)
        for token in stmt.flatten():
            if token.ttype in (DDL, DML) and token.normalized.upper() != "SELECT":
                raise SQLValidationError(
                    f"Disallowed token '{token.normalized}' in SQL."
                )
