"""Build the LLM prompt from schema context and user question."""
from __future__ import annotations

from typing import Any, Optional

from app.core.schema_analyzer import RelationshipGraph


_SYSTEM_PROMPT = """\
You are an expert SQL analyst. Your sole job is to write a single, correct, \
read-only SELECT statement that answers the user's question.

Rules:
- Output ONLY a SQL code block and nothing else (no explanations).
- The query MUST be a SELECT statement — no INSERT, UPDATE, DELETE, DROP, \
  CREATE, ALTER, TRUNCATE, or any DDL/DML.
- Do not use semicolons at the end.
- If you cannot answer with a SELECT query, output: SELECT 'unable to answer' AS message
"""


class PromptBuilder:
    def __init__(
        self,
        schema: dict[str, Any],
        graph: RelationshipGraph | None = None,
        row_counts: dict[str, int] | None = None,
        db_dialect: str = "SQL",
        example_store=None,
        few_shot_k: int = 3,
    ) -> None:
        self._schema = schema
        self._graph = graph or RelationshipGraph()
        self._row_counts = row_counts or {}
        self._db_dialect = db_dialect
        self._example_store = example_store
        self._few_shot_k = few_shot_k
        self._last_selected: list[str] = []

    @property
    def system_prompt(self) -> str:
        return _SYSTEM_PROMPT + f"\n- Database dialect: {self._db_dialect}"

    @property
    def last_selected_tables(self) -> list[str]:
        """Tables chosen for the most recent build() call. Useful for trace logging."""
        return list(self._last_selected)

    def build(
        self,
        question: str,
        conversation_context: str = "",
    ) -> list[dict[str, str]]:
        """Return a messages list ready for the LLM API.

        Args:
            question:               Natural language question.
            conversation_context:   Optional block from ConversationSession.context_block()
                                    injected for multi-turn follow-ups.
        """
        tables = self._select_tables(question)
        self._last_selected = tables
        schema_block = self._render_schema(tables)
        relationships_block = self._render_relationships(tables)

        parts = [f"Database schema:\n{schema_block}"]
        if relationships_block:
            parts.append(f"Table relationships:\n{relationships_block}")

        # Few-shot examples retrieved from the example store
        if self._example_store:
            examples = self._example_store.retrieve(question, k=self._few_shot_k)
            if examples:
                parts.append(self._render_examples(examples))

        # Multi-turn context (previous SQL + columns for follow-ups)
        if conversation_context:
            parts.append(conversation_context)

        parts.append(f"Question: {question}\n\nWrite the SQL query:")

        return [{"role": "user", "content": "\n\n".join(parts)}]

    # ------------------------------------------------------------------
    # Table selection — keyword match + relationship expansion
    # ------------------------------------------------------------------

    def _select_tables(self, question: str) -> list[str]:
        q_lower = question.lower()
        matched: set[str] = set()

        for table_name, meta in self._schema.items():
            if table_name.lower() in q_lower:
                matched.add(table_name)
                continue
            for col in meta.get("columns", []):
                if col["name"].lower() in q_lower:
                    matched.add(table_name)
                    break

        # Expand: pull in any table that's directly connected to a matched table.
        # This ensures the LLM gets JOIN-able tables without the user having to
        # name them explicitly ("show me user orders" → need both users + orders).
        if matched:
            neighbors: set[str] = set()
            for t in matched:
                for n in self._graph.neighbors(t):
                    if n in self._schema:
                        neighbors.add(n)
            matched.update(neighbors)

        return sorted(matched) if matched else sorted(self._schema.keys())

    # ------------------------------------------------------------------
    # Rendering
    # ------------------------------------------------------------------

    def _render_schema(self, tables: list[str]) -> str:
        lines: list[str] = []
        for table_name in tables:
            meta = self._schema[table_name]

            count = self._row_counts.get(table_name)
            count_str = f"  (~{count:,} rows)" if count is not None and count >= 0 else ""
            lines.append(f"Table: {table_name}{count_str}")

            pk_set = set(meta.get("primary_keys", []))
            for col in meta.get("columns", []):
                pk_marker = " [PK]" if col["name"] in pk_set else ""
                lines.append(f"  - {col['name']} ({col['type']}){pk_marker}")

            samples = meta.get("sample_rows", [])
            if samples:
                lines.append("  Sample rows:")
                for row in samples[:3]:
                    lines.append(f"    {row}")

            lines.append("")

        return "\n".join(lines)

    def _render_examples(self, examples: list) -> str:
        """Render retrieved few-shot examples as a prompt block."""
        lines = ["Reference examples (similar past queries — adapt as needed):"]
        for ex in examples:
            lines.append(f"\nQ: {ex.question}")
            lines.append(f"A:\n```sql\n{ex.sql}\n```")
        return "\n".join(lines)

    def _render_relationships(self, tables: list[str]) -> str:
        table_set = set(tables)
        relevant = [
            e for e in self._graph.edges
            if e.from_table in table_set and e.to_table in table_set
        ]
        if not relevant:
            return ""

        lines = []
        for e in relevant:
            hint = self._graph.join_hint(e.from_table, e.to_table)
            tag = "(declared FK)" if e.source == "explicit_fk" else "(inferred)"
            lines.append(f"  {hint}  {tag}")
        return "\n".join(lines)
