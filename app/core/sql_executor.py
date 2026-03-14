"""Execute a validated SELECT query and return a pandas DataFrame."""
from __future__ import annotations

import pandas as pd
from sqlalchemy import Engine, text


class SQLExecutionError(Exception):
    pass


class SQLExecutor:
    def __init__(self, engine: Engine, max_rows: int | None = None) -> None:
        self._engine = engine
        from config import settings
        self._max_rows = max_rows if max_rows is not None else settings.max_rows

    def execute(self, sql: str) -> pd.DataFrame:
        """Run *sql* and return a DataFrame, capped at *max_rows* rows."""
        try:
            with self._engine.connect() as conn:
                # Wrap in a read-only savepoint so no implicit writes leak through
                with conn.begin():
                    result = conn.execute(text(sql))
                    rows = result.fetchmany(self._max_rows)
                    columns = list(result.keys())

            df = pd.DataFrame(rows, columns=columns)
            return df
        except Exception as exc:
            raise SQLExecutionError(str(exc)) from exc
