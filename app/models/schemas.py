from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


class ChartConfig(BaseModel):
    type: str  # bar | line | scatter | pie
    x: str
    y: str
    title: str


class CorrectionRecord(BaseModel):
    attempt: int
    sql: str
    error: str


class PipelineTrace(BaseModel):
    tables_selected: list[str]
    relationships_used: list[str]
    schema_issues: list[str]
    attempts: int = 1
    correction_history: list[CorrectionRecord] = []


class QueryRequest(BaseModel):
    question: str = Field(..., min_length=1, description="Natural language question")
    database_url: Optional[str] = Field(
        None, description="Override the default DATABASE_URL for this request"
    )
    provider: Optional[Literal["anthropic", "openai"]] = Field(
        None, description="Override the default LLM provider for this request"
    )
    session_id: Optional[str] = Field(
        None, description="Session ID for multi-turn conversation"
    )


# ---------------------------------------------------------------------------
# Session models
# ---------------------------------------------------------------------------

class SessionCreateRequest(BaseModel):
    database_url: Optional[str] = None


class SessionResponse(BaseModel):
    session_id: str
    turn_count: int


class ConversationTurnResponse(BaseModel):
    turn: int
    question: str
    sql: str
    row_count: int
    summary: str


class SessionHistoryResponse(BaseModel):
    session_id: str
    turns: list[ConversationTurnResponse]


class QueryResponse(BaseModel):
    question: str
    sql: str
    rows: list[dict[str, Any]]
    row_count: int
    summary: str
    chart: Optional[ChartConfig] = None
    execution_time_ms: float
    trace: PipelineTrace
    confidence: float = 1.0


# ---------------------------------------------------------------------------
# Schema graph
# ---------------------------------------------------------------------------

class GraphNode(BaseModel):
    id: str                        # table name
    columns: list[str]             # column names in order
    primary_keys: list[str]
    row_count: int = -1


class GraphEdge(BaseModel):
    from_table: str
    from_col: str
    to_table: str
    to_col: str
    source: str                    # "explicit_fk" | "heuristic"


class SchemaGraphResponse(BaseModel):
    nodes: list[GraphNode]
    edges: list[GraphEdge]


class SchemaColumnInfo(BaseModel):
    name: str
    type: str
    nullable: bool


class TableSchema(BaseModel):
    columns: list[SchemaColumnInfo]
    primary_keys: list[str]
    foreign_keys: list[dict[str, Any]]
    sample_rows: list[dict[str, Any]]


class SchemaResponse(BaseModel):
    tables: dict[str, TableSchema]


class HealthResponse(BaseModel):
    status: str
    db: str
