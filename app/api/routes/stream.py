"""Streaming SSE endpoint for the NL-to-SQL pipeline.

Sends a sequence of typed events so the frontend can render each pipeline
stage in real-time:

  {"type": "status",     "data": "Analyzing schema…"}
  {"type": "generating", "data": null}           — SQL tokens are about to start
  {"type": "token",      "data": "SELECT "}      — one token at a time
  {"type": "correction", "data": {"attempt":1, "error":"..."}}
  {"type": "status",     "data": "Running query…"}
  {"type": "result",     "data": <QueryResponse dict>}
  {"type": "error",      "data": "human-readable message"}
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import AsyncIterator

from fastapi import APIRouter
from sqlalchemy import create_engine
from sse_starlette.sse import EventSourceResponse

from app.db.connection import get_engine
from app.core.schema_loader import SchemaLoader
from app.core.schema_analyzer import SchemaAnalyzer
from app.core.prompt_builder import PromptBuilder
from app.core.sql_generator import SQLGenerator
from app.core.sql_validator import SQLValidator, SQLValidationError
from app.core.sql_executor import SQLExecutor, SQLExecutionError
from app.core.schema_validator import SchemaValidator
from app.core.result_formatter import ResultFormatter
from app.core.pipeline import _CORRECTION_TEMPLATE
from app.core.stores import example_store, query_cache
from app.core.confidence import compute_confidence
from app.core.conversation import session_store, ConversationTurn
from app.core.performance_hints import analyze_performance
from app.models.schemas import QueryRequest, ChartConfig, PipelineTrace, CorrectionRecord

logger = logging.getLogger(__name__)

router = APIRouter(tags=["stream"])

_sql_validator = SQLValidator()
_schema_validator = SchemaValidator()
MAX_RETRIES = 2


def _evt(type_: str, data=None) -> dict:
    return {"data": json.dumps({"type": type_, "data": data})}


@router.post("/ask/stream")
async def ask_stream(request: QueryRequest):
    """SSE endpoint — streams pipeline progress and SQL tokens to the client."""

    engine = (
        create_engine(request.database_url)
        if request.database_url
        else get_engine()
    )
    provider = request.provider
    generator = SQLGenerator(provider=provider)
    formatter = ResultFormatter(provider=provider)
    executor = SQLExecutor(engine)
    loop = asyncio.get_event_loop()

    # Resolve session for multi-turn context
    session = None
    if request.session_id:
        session = session_store.get(request.session_id)
        if not session:
            async def _err():
                yield _evt("error", f"Session '{request.session_id}' not found")
            return EventSourceResponse(_err())

    async def event_stream() -> AsyncIterator[dict]:
        try:
            # ── 0. Semantic cache check ───────────────────────────────────
            cache_db_id = request.database_url or "default"
            cached = await loop.run_in_executor(
                None, lambda: query_cache.lookup(request.question, db_id=cache_db_id)
            )
            if cached:
                yield _evt("status", "Cache hit — returning cached result…")
                yield _evt("result", {**cached.payload, "cache_hit": True})
                return

            # ── 1. Schema analysis (sync → thread) ───────────────────────
            yield _evt("status", "Analyzing schema…")
            schema = await loop.run_in_executor(None, lambda: SchemaLoader(engine).load())

            analyzer = SchemaAnalyzer(schema, engine)
            graph = await loop.run_in_executor(None, analyzer.build_graph)
            counts = await loop.run_in_executor(None, analyzer.row_counts)

            conversation_context = session.context_block() if session else ""
            builder = PromptBuilder(schema, graph=graph, row_counts=counts, example_store=example_store)
            messages = builder.build(request.question, conversation_context=conversation_context)
            tables_selected = builder.last_selected_tables

            table_set = set(tables_selected)
            relationships_used = [
                str(e) for e in graph.edges
                if e.from_table in table_set and e.to_table in table_set
            ]

            # ── 2. Self-correcting generation + execution loop ────────────
            correction_history: list[CorrectionRecord] = []
            current_messages = list(messages)
            sql = None
            df = None

            for attempt in range(1, MAX_RETRIES + 2):
                yield _evt("generating", None)
                raw_tokens: list[str] = []

                async for token in generator.astream_generate(current_messages):
                    raw_tokens.append(token)
                    yield _evt("token", token)

                raw_text = "".join(raw_tokens)
                sql = SQLGenerator._extract_sql(raw_text)

                if not sql:
                    yield _evt("error", "Could not extract SQL from model response.")
                    return

                try:
                    _sql_validator.validate(sql)
                except SQLValidationError as exc:
                    yield _evt("error", f"Unsafe SQL: {exc}")
                    return

                yield _evt("status", "Running query…")
                try:
                    df = await loop.run_in_executor(None, executor.execute, sql)
                    break

                except SQLExecutionError as exc:
                    error_msg = str(exc)
                    logger.warning("Attempt %d failed: %s", attempt, error_msg[:120])

                    if attempt > MAX_RETRIES:
                        yield _evt("error", f"All attempts exhausted. Last error: {error_msg}")
                        return

                    correction_history.append(
                        CorrectionRecord(attempt=attempt, sql=sql, error=error_msg)
                    )
                    yield _evt("correction", {"attempt": attempt, "error": error_msg})

                    current_messages = current_messages + [
                        {"role": "assistant", "content": f"```sql\n{sql}\n```"},
                        {"role": "user", "content": _CORRECTION_TEMPLATE.format(
                            sql=sql, error=error_msg,
                        )},
                    ]

            # ── 3. Schema soft-validation ─────────────────────────────────
            schema_issues = await loop.run_in_executor(
                None, lambda: _schema_validator.validate(sql, schema)
            )
            table_errors = [
                i for i in schema_issues if i.severity == "error" and "Table" in i.message
            ]
            if table_errors:
                detail = "; ".join(str(i) for i in table_errors)
                yield _evt("error", f"Schema error: {detail}")
                return

            # ── 4. Format result (may call LLM for narrative) ─────────────
            yield _evt("status", "Generating narrative…")
            formatted = await loop.run_in_executor(
                None, lambda: formatter.format(df, request.question, sql, 0.0)
            )

            chart_data = formatted.get("chart")
            chart = ChartConfig(**chart_data).model_dump() if chart_data else None

            trace = PipelineTrace(
                tables_selected=tables_selected,
                relationships_used=relationships_used,
                schema_issues=[str(i) for i in schema_issues],
                attempts=len(correction_history) + 1,
                correction_history=correction_history,
            )

            confidence = compute_confidence(
                schema_issues=schema_issues,
                attempts=len(correction_history) + 1,
                row_count=formatted["row_count"],
            )

            # ── 5. Performance hints ──────────────────────────────────────
            perf_hints = await loop.run_in_executor(
                None, lambda: analyze_performance(sql, schema)
            )

            result_payload = {
                "question": formatted["question"],
                "sql": formatted["sql"],
                "rows": formatted["rows"],
                "row_count": formatted["row_count"],
                "summary": formatted["summary"],
                "chart": chart,
                "execution_time_ms": formatted["execution_time_ms"],
                "trace": trace.model_dump(),
                "confidence": confidence,
                "cache_hit": False,
                "performance_hints": [str(h) for h in perf_hints],
                "ambiguity_warning": None,
            }

            # Persist turn to session
            if session:
                await loop.run_in_executor(None, lambda: session.add_turn(ConversationTurn(
                    question=request.question,
                    sql=sql,
                    result_columns=list(df.columns),
                    row_count=formatted["row_count"],
                    summary=formatted["summary"],
                )))

            # Store for few-shot retrieval and semantic cache
            await loop.run_in_executor(
                None, lambda: example_store.add(request.question, sql, db_id="default")
            )
            await loop.run_in_executor(
                None, lambda: query_cache.store(request.question, result_payload, db_id=cache_db_id)
            )

            yield _evt("result", result_payload)

        except Exception as exc:
            logger.exception("Unexpected error in stream endpoint")
            yield _evt("error", str(exc))

    return EventSourceResponse(event_stream())
