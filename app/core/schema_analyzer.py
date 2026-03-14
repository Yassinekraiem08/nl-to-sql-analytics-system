"""Build a relationship graph from schema metadata and live row counts."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import Engine, text


@dataclass
class JoinEdge:
    from_table: str
    from_col: str
    to_table: str
    to_col: str
    source: str  # "explicit_fk" | "heuristic"

    def __str__(self) -> str:
        tag = "FK" if self.source == "explicit_fk" else "~"
        return f"{self.from_table}.{self.from_col} {tag}→ {self.to_table}.{self.to_col}"


@dataclass
class RelationshipGraph:
    edges: list[JoinEdge] = field(default_factory=list)

    def neighbors(self, table: str) -> list[str]:
        """All tables directly reachable from *table* via any edge."""
        result: set[str] = set()
        for e in self.edges:
            if e.from_table == table:
                result.add(e.to_table)
            elif e.to_table == table:
                result.add(e.from_table)
        return sorted(result)

    def join_hint(self, t1: str, t2: str) -> str | None:
        """Return the ON condition string for joining t1 and t2, or None."""
        for e in self.edges:
            if e.from_table == t1 and e.to_table == t2:
                return f"{t1}.{e.from_col} = {t2}.{e.to_col}"
            if e.from_table == t2 and e.to_table == t1:
                return f"{t2}.{e.from_col} = {t1}.{e.to_col}"
        return None

    def render(self) -> str:
        """Human-readable summary for debugging and prompts."""
        if not self.edges:
            return "(no relationships detected)"
        return "\n".join(f"  {e}" for e in self.edges)


class SchemaAnalyzer:
    """
    Turns raw schema metadata into something the rest of the system can
    reason with: a relationship graph and per-table row counts.

    Relationship detection runs in two passes:
      1. Explicit FK constraints declared in the database.
      2. Heuristic: any column named <table>_id that matches a known table.
         Covers the very common case where the DB doesn't enforce FKs (SQLite,
         poorly-migrated Postgres, etc.) but the naming convention is there.
    """

    _FK_SUFFIX = re.compile(r"^(.+)_id$", re.IGNORECASE)

    def __init__(self, schema: dict[str, Any], engine: Engine) -> None:
        self._schema = schema
        self._engine = engine

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build_graph(self) -> RelationshipGraph:
        graph = RelationshipGraph()
        table_names = set(self._schema.keys())
        self._add_explicit_fks(graph)
        self._add_heuristic_fks(graph, table_names, already_covered=graph.edges[:])
        return graph

    def row_counts(self) -> dict[str, int]:
        """Run one COUNT(*) per table. Returns -1 for any table that fails."""
        counts: dict[str, int] = {}
        for table in self._schema:
            try:
                with self._engine.connect() as conn:
                    quoted = self._engine.dialect.identifier_preparer.quote(table)
                    val = conn.execute(text(f"SELECT COUNT(*) FROM {quoted}")).scalar()
                    counts[table] = int(val or 0)
            except Exception:
                counts[table] = -1
        return counts

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _add_explicit_fks(self, graph: RelationshipGraph) -> None:
        for table, meta in self._schema.items():
            for fk in meta.get("foreign_keys", []):
                for col, ref_col in zip(
                    fk.get("columns", []), fk.get("referred_columns", [])
                ):
                    graph.edges.append(
                        JoinEdge(
                            from_table=table,
                            from_col=col,
                            to_table=fk["referred_table"],
                            to_col=ref_col,
                            source="explicit_fk",
                        )
                    )

    def _add_heuristic_fks(
        self,
        graph: RelationshipGraph,
        table_names: set[str],
        already_covered: list[JoinEdge],
    ) -> None:
        covered = {(e.from_table, e.from_col) for e in already_covered}

        for table, meta in self._schema.items():
            for col in meta.get("columns", []):
                col_name: str = col["name"]
                if (table, col_name) in covered:
                    continue

                match = self._FK_SUFFIX.match(col_name)
                if not match:
                    continue

                candidate = match.group(1).lower()
                ref_table = self._resolve_table(candidate, table_names, exclude=table)
                if ref_table is None:
                    continue

                pk = self._schema[ref_table].get("primary_keys", [])
                ref_col = pk[0] if pk else "id"

                graph.edges.append(
                    JoinEdge(
                        from_table=table,
                        from_col=col_name,
                        to_table=ref_table,
                        to_col=ref_col,
                        source="heuristic",
                    )
                )

    @staticmethod
    def _resolve_table(
        candidate: str, table_names: set[str], exclude: str
    ) -> str | None:
        """Try candidate, candidate+'s', candidate without trailing 's'."""
        lower_map = {t.lower(): t for t in table_names}
        for variant in (candidate, candidate + "s", candidate.rstrip("s")):
            resolved = lower_map.get(variant)
            if resolved and resolved != exclude:
                return resolved
        return None
