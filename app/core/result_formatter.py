"""Convert a DataFrame result into a structured response with summary and chart config."""
from __future__ import annotations

from typing import Any

import pandas as pd

from app.core.llm_client import LLMClient
from config import settings


class ResultFormatter:
    def __init__(self, model: str | None = None, provider: str | None = None) -> None:
        self._client = LLMClient(model=model, provider=provider)

    def format(
        self,
        df: pd.DataFrame,
        question: str,
        sql: str,
        execution_time_ms: float,
    ) -> dict[str, Any]:
        rows = df.to_dict(orient="records")
        summary = self._summarize(question, sql, df)
        chart = self._recommend_chart(df)

        return {
            "question": question,
            "sql": sql,
            "rows": rows,
            "row_count": len(df),
            "summary": summary,
            "chart": chart,
            "execution_time_ms": round(execution_time_ms, 2),
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _summarize(self, question: str, sql: str, df: pd.DataFrame) -> str:
        """Ask the LLM to explain the query result in plain English."""
        preview = df.head(10).to_string(index=False) if not df.empty else "(no rows returned)"
        prompt = (
            f"A user asked: \"{question}\"\n\n"
            f"The following SQL was executed:\n```sql\n{sql}\n```\n\n"
            f"Result preview ({len(df)} rows total):\n{preview}\n\n"
            "Write a concise, plain-English answer to the user's question based on these results. "
            "Be factual and brief (2–4 sentences)."
        )
        try:
            return self._client.complete(
                [{"role": "user", "content": prompt}],
                max_tokens=settings.max_tokens_summary,
            )
        except Exception as exc:
            return f"(Summary unavailable: {exc})"

    @staticmethod
    def _recommend_chart(df: pd.DataFrame) -> dict[str, Any] | None:
        """Heuristically recommend a Plotly chart type based on column types."""
        if df.empty or len(df.columns) < 2:
            return None

        numeric_cols = df.select_dtypes(include="number").columns.tolist()
        non_numeric_cols = df.select_dtypes(exclude="number").columns.tolist()

        if not numeric_cols:
            return None

        x_col = non_numeric_cols[0] if non_numeric_cols else df.columns[0]
        y_col = numeric_cols[0]

        # Time-series detection
        x_lower = str(x_col).lower()
        if any(kw in x_lower for kw in ("date", "time", "month", "year", "week", "day")):
            chart_type = "line"
        elif len(df) <= 10 and len(numeric_cols) == 1:
            chart_type = "bar"
        elif len(numeric_cols) >= 2:
            chart_type = "scatter"
        else:
            chart_type = "bar"

        return {
            "type": chart_type,
            "x": x_col,
            "y": y_col,
            "title": f"{y_col} by {x_col}",
        }
