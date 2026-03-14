"""Data export endpoint — CSV and Excel.

POST /export
    { "sql": "SELECT …", "format": "csv"|"excel", "filename": "…", "database_url": "…" }

Returns a streamed file download so the user can save query results
without re-running the full NL-to-SQL pipeline.
"""
from __future__ import annotations

import io
import logging
from typing import Literal, Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text

from app.db.connection import get_engine
from app.core.sql_validator import SQLValidator, SQLValidationError

logger = logging.getLogger(__name__)

router = APIRouter(tags=["export"])

_validator = SQLValidator()


class ExportRequest(BaseModel):
    sql: str = Field(..., min_length=1)
    format: Literal["csv", "excel"] = "csv"
    filename: str = "query_results"
    database_url: Optional[str] = None


@router.post("/export")
def export_data(req: ExportRequest) -> StreamingResponse:
    """Execute SQL and stream results as CSV or Excel."""
    try:
        _validator.validate(req.sql)
    except SQLValidationError as exc:
        raise HTTPException(status_code=422, detail=f"Unsafe SQL: {exc}")

    engine = create_engine(req.database_url) if req.database_url else get_engine()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(req.sql))
            columns = list(result.keys())
            rows = [dict(zip(columns, row)) for row in result.fetchall()]
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Query failed: {exc}")

    if req.format == "csv":
        return _csv_response(columns, rows, req.filename)
    return _excel_response(columns, rows, req.filename)


# ---------------------------------------------------------------------------
# Formatters
# ---------------------------------------------------------------------------

def _csv_response(
    columns: list[str], rows: list[dict], filename: str
) -> StreamingResponse:
    import csv

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=columns)
    writer.writeheader()
    writer.writerows(rows)
    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}.csv"'},
    )


def _excel_response(
    columns: list[str], rows: list[dict], filename: str
) -> StreamingResponse:
    try:
        import openpyxl
        from openpyxl.styles import Font
    except ImportError:
        raise HTTPException(
            status_code=501,
            detail="openpyxl is required for Excel export. Run: pip install openpyxl",
        )

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Results"

    ws.append(columns)
    for cell in ws[1]:
        cell.font = Font(bold=True)

    for row in rows:
        ws.append([row.get(col) for col in columns])

    # Auto-size columns (capped at 50)
    for col in ws.columns:
        max_len = max((len(str(cell.value or "")) for cell in col), default=0)
        ws.column_dimensions[col[0].column_letter].width = min(max_len + 2, 50)

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type=(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
        headers={"Content-Disposition": f'attachment; filename="{filename}.xlsx"'},
    )
