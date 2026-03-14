from fastapi import APIRouter, HTTPException

from app.db.connection import get_engine
from app.core.schema_loader import SchemaLoader
from app.core.schema_analyzer import SchemaAnalyzer
from app.models.schemas import SchemaResponse, TableSchema, SchemaGraphResponse, GraphNode, GraphEdge

router = APIRouter(prefix="/schema", tags=["schema"])


def _get_loader() -> SchemaLoader:
    return SchemaLoader(get_engine())


@router.get("", response_model=SchemaResponse)
def get_full_schema() -> SchemaResponse:
    schema = _get_loader().load()
    return SchemaResponse(tables={k: TableSchema(**v) for k, v in schema.items()})


@router.get("/graph", response_model=SchemaGraphResponse)
def get_schema_graph() -> SchemaGraphResponse:
    """Return the full relationship graph — nodes (tables) and edges (FKs)."""
    engine = get_engine()
    schema = _get_loader().load()
    analyzer = SchemaAnalyzer(schema, engine)
    graph = analyzer.build_graph()
    counts = analyzer.row_counts()

    nodes = [
        GraphNode(
            id=name,
            columns=[c["name"] for c in meta.get("columns", [])],
            primary_keys=meta.get("primary_keys", []),
            row_count=counts.get(name, -1),
        )
        for name, meta in schema.items()
    ]

    edges = [
        GraphEdge(
            from_table=e.from_table,
            from_col=e.from_col,
            to_table=e.to_table,
            to_col=e.to_col,
            source=e.source,
        )
        for e in graph.edges
    ]

    return SchemaGraphResponse(nodes=nodes, edges=edges)


@router.get("/{table_name}", response_model=TableSchema)
def get_table_schema(table_name: str) -> TableSchema:
    try:
        meta = _get_loader().table(table_name)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return TableSchema(**meta)
