"""Introspect a live database and extract table/column metadata."""
from __future__ import annotations

import functools
from typing import Any

from sqlalchemy import Engine, inspect, text


class SchemaLoader:
    def __init__(self, engine: Engine, sample_rows: int = 3) -> None:
        self._engine = engine
        self._sample_rows = sample_rows
        self._cache: dict[str, Any] | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self, force_refresh: bool = False) -> dict[str, Any]:
        """Return the full schema dict, using cache unless *force_refresh*."""
        if self._cache is None or force_refresh:
            self._cache = self._introspect()
        return self._cache

    def table(self, table_name: str) -> dict[str, Any]:
        schema = self.load()
        if table_name not in schema:
            raise KeyError(f"Table '{table_name}' not found in schema.")
        return schema[table_name]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _introspect(self) -> dict[str, Any]:
        inspector = inspect(self._engine)
        schema: dict[str, Any] = {}

        for table_name in inspector.get_table_names():
            columns = []
            for col in inspector.get_columns(table_name):
                columns.append(
                    {
                        "name": col["name"],
                        "type": str(col["type"]),
                        "nullable": col.get("nullable", True),
                    }
                )

            pk_info = inspector.get_pk_constraint(table_name)
            pk_cols: list[str] = pk_info.get("constrained_columns", []) if pk_info else []

            fk_list = []
            for fk in inspector.get_foreign_keys(table_name):
                fk_list.append(
                    {
                        "columns": fk.get("constrained_columns", []),
                        "referred_table": fk.get("referred_table"),
                        "referred_columns": fk.get("referred_columns", []),
                    }
                )

            sample = self._fetch_sample(table_name)

            schema[table_name] = {
                "columns": columns,
                "primary_keys": pk_cols,
                "foreign_keys": fk_list,
                "sample_rows": sample,
            }

        return schema

    def _fetch_sample(self, table_name: str) -> list[dict[str, Any]]:
        try:
            with self._engine.connect() as conn:
                # Quote identifiers to handle reserved words
                quoted = self._engine.dialect.identifier_preparer.quote(table_name)
                rows = conn.execute(
                    text(f"SELECT * FROM {quoted} LIMIT :n"),
                    {"n": self._sample_rows},
                ).fetchall()
                keys = [col for col in conn.execute(
                    text(f"SELECT * FROM {quoted} LIMIT 0")
                ).keys()]
                return [dict(zip(keys, row)) for row in rows]
        except Exception:
            return []
