from fastapi import APIRouter, HTTPException
from sqlalchemy import create_engine

from app.db.connection import get_engine
from app.core.schema_loader import SchemaLoader
from app.core.schema_analyzer import SchemaAnalyzer
from app.core.prompt_builder import PromptBuilder
from app.core.sql_generator import SQLGenerator
from app.core.sql_validator import SQLValidator, SQLValidationError
from app.core.schema_validator import SchemaValidator
from app.core.sql_executor import SQLExecutor
from app.core.pipeline import SelfCorrectingPipeline, PipelineError
from app.core.result_formatter import ResultFormatter
from app.core.stores import example_store, query_cache
from app.core.confidence import compute_confidence
from app.core.conversation import session_store, ConversationTurn
from app.core.ambiguity import detect_ambiguity
from app.core.performance_hints import analyze_performance
from app.models.schemas import (
    QueryRequest, QueryResponse, ChartConfig, PipelineTrace, CorrectionRecord,
    SessionCreateRequest, SessionResponse, SessionHistoryResponse, ConversationTurnResponse,
)

router = APIRouter(tags=["query"])

_sql_validator = SQLValidator()
_schema_validator = SchemaValidator()
_generator = SQLGenerator()
_formatter = ResultFormatter()


# ---------------------------------------------------------------------------
# Session management endpoints
# ---------------------------------------------------------------------------

@router.post("/sessions", response_model=SessionResponse, status_code=201)
def create_session(body: SessionCreateRequest = SessionCreateRequest()) -> SessionResponse:
    """Create a new conversation session. Returns a session_id to use in /ask."""
    session = session_store.create(database_url=body.database_url)
    return SessionResponse(session_id=session.session_id, turn_count=0)


@router.get("/sessions/{session_id}", response_model=SessionHistoryResponse)
def get_session(session_id: str) -> SessionHistoryResponse:
    session = session_store.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return SessionHistoryResponse(
        session_id=session_id,
        turns=[
            ConversationTurnResponse(
                turn=i + 1,
                question=t.question,
                sql=t.sql,
                row_count=t.row_count,
                summary=t.summary,
            )
            for i, t in enumerate(session.turns)
        ],
    )


@router.delete("/sessions/{session_id}", status_code=204)
def delete_session(session_id: str) -> None:
    if not session_store.delete(session_id):
        raise HTTPException(status_code=404, detail="Session not found")


# ---------------------------------------------------------------------------
# Main query endpoint (stateless + stateful)
# ---------------------------------------------------------------------------

@router.post("/ask", response_model=QueryResponse)
def ask(request: QueryRequest) -> QueryResponse:
    # --- Resolve session and database ---
    session = None
    if request.session_id:
        session = session_store.get(request.session_id)
        if not session:
            raise HTTPException(status_code=404, detail=f"Session '{request.session_id}' not found")

    db_url = (request.database_url
              or (session.database_url if session else None))
    engine = create_engine(db_url) if db_url else get_engine()

    # --- Semantic cache check (skip LLM if near-identical question seen before) ---
    cache_db_id = db_url or "default"
    cached = query_cache.lookup(request.question, db_id=cache_db_id)
    if cached:
        return QueryResponse(**{**cached.payload, "cache_hit": True})

    # --- Ambiguity detection (non-blocking — attaches warning to response) ---
    ambiguity = detect_ambiguity(request.question)
    ambiguity_warning = ambiguity.warning_text() if ambiguity.is_ambiguous else None

    # --- Schema intelligence ---
    schema = SchemaLoader(engine).load()
    analyzer = SchemaAnalyzer(schema, engine)
    graph = analyzer.build_graph()
    counts = analyzer.row_counts()

    # --- Build prompt (with conversation context for follow-ups) ---
    conversation_context = session.context_block() if session else ""
    builder = PromptBuilder(schema, graph=graph, row_counts=counts, example_store=example_store)
    messages = builder.build(request.question, conversation_context=conversation_context)
    tables_selected = builder.last_selected_tables

    table_set = set(tables_selected)
    relationships_used = [
        str(e) for e in graph.edges
        if e.from_table in table_set and e.to_table in table_set
    ]

    # --- Self-correcting pipeline ---
    provider = request.provider
    generator = SQLGenerator(provider=provider) if provider else _generator
    formatter = ResultFormatter(provider=provider) if provider else _formatter

    pipeline = SelfCorrectingPipeline(
        generator=generator,
        validator=_sql_validator,
        executor=SQLExecutor(engine),
        max_retries=2,
    )

    try:
        result = pipeline.run(messages)
    except SQLValidationError as exc:
        raise HTTPException(status_code=422, detail=f"Unsafe SQL: {exc}")
    except PipelineError as exc:
        raise HTTPException(status_code=500, detail=f"Pipeline error: {exc}")

    sql = result.sql
    df = result.df

    # --- Schema validation ---
    schema_issues = _schema_validator.validate(sql, schema)
    table_errors = [i for i in schema_issues if i.severity == "error" and "Table" in i.message]
    if table_errors:
        detail = "; ".join(str(i) for i in table_errors)
        raise HTTPException(status_code=422, detail=f"Schema error: {detail}")

    formatted = formatter.format(df, request.question, sql, result.elapsed_ms)

    chart_data = formatted.get("chart")
    chart = ChartConfig(**chart_data) if chart_data else None

    trace = PipelineTrace(
        tables_selected=tables_selected,
        relationships_used=relationships_used,
        schema_issues=[str(i) for i in schema_issues],
        attempts=result.attempts,
        correction_history=[
            CorrectionRecord(attempt=s.attempt, sql=s.sql, error=s.error)
            for s in result.correction_history
        ],
    )

    confidence = compute_confidence(
        schema_issues=schema_issues,
        attempts=result.attempts,
        row_count=formatted["row_count"],
    )

    # --- Performance hints ---
    perf_hints = analyze_performance(sql, schema)

    # --- Persist to session and example store ---
    if session:
        session.add_turn(ConversationTurn(
            question=request.question,
            sql=sql,
            result_columns=list(df.columns),
            row_count=formatted["row_count"],
            summary=formatted["summary"],
        ))

    example_store.add(request.question, sql, db_id="default")

    # --- Store result in semantic cache ---
    result_payload = {
        "question": formatted["question"],
        "sql": formatted["sql"],
        "rows": formatted["rows"],
        "row_count": formatted["row_count"],
        "summary": formatted["summary"],
        "chart": chart.model_dump() if chart else None,
        "execution_time_ms": formatted["execution_time_ms"],
        "trace": trace.model_dump(),
        "confidence": confidence,
        "cache_hit": False,
        "performance_hints": [str(h) for h in perf_hints],
        "ambiguity_warning": ambiguity_warning,
    }
    query_cache.store(request.question, result_payload, db_id=cache_db_id)

    return QueryResponse(
        question=formatted["question"],
        sql=formatted["sql"],
        rows=formatted["rows"],
        row_count=formatted["row_count"],
        summary=formatted["summary"],
        chart=chart,
        execution_time_ms=formatted["execution_time_ms"],
        trace=trace,
        confidence=confidence,
        cache_hit=False,
        performance_hints=[str(h) for h in perf_hints],
        ambiguity_warning=ambiguity_warning,
    )
