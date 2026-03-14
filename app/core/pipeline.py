"""Self-correcting NL-to-SQL execution pipeline.

On execution failure the pipeline feeds the error message back to the LLM
and requests a corrected query, repeating up to `max_retries` times.

This technique is called *execution-guided refinement* and is described in:
  - DIN-SQL (Pourreza & Rafiei, 2023)
  - DAIL-SQL (Gao et al., 2023)
  - CHESS (Talaei et al., 2024)
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

from app.core.sql_generator import SQLGenerator, SQLGenerationError
from app.core.sql_validator import SQLValidator, SQLValidationError
from app.core.sql_executor import SQLExecutor, SQLExecutionError

logger = logging.getLogger(__name__)

_CORRECTION_TEMPLATE = """\
The SQL query you generated produced the following error when executed:

```sql
{sql}
```

Error: {error}

Analyse the error carefully and write a corrected SQL query that answers \
the original question. Output ONLY the corrected SQL in a code block — \
no explanations, no apologies.
"""


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class CorrectionStep:
    """One failed attempt recorded in the correction history."""
    attempt: int
    sql: str
    error: str


@dataclass
class PipelineResult:
    """Successful result returned by SelfCorrectingPipeline.run()."""
    sql: str
    df: pd.DataFrame
    elapsed_ms: float
    attempts: int                                  # 1 = succeeded first try
    correction_history: list[CorrectionStep] = field(default_factory=list)


class PipelineError(Exception):
    """Raised when all retry attempts are exhausted."""
    def __init__(self, message: str, last_sql: Optional[str] = None):
        super().__init__(message)
        self.last_sql = last_sql


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

class SelfCorrectingPipeline:
    """Execute the NL-to-SQL pipeline with automatic error-driven correction.

    Flow per attempt:
      1. Call LLM to generate SQL
      2. Safety-validate (SELECT-only) — abort immediately if unsafe
      3. Execute against the database
      4. On SQLExecutionError: append error + SQL to messages, retry

    Args:
        generator:   SQLGenerator instance
        validator:   SQLValidator (safety) instance
        executor:    SQLExecutor instance
        max_retries: correction attempts after the first failure
                     (total attempts = max_retries + 1)
    """

    def __init__(
        self,
        generator: SQLGenerator,
        validator: SQLValidator,
        executor: SQLExecutor,
        max_retries: int = 2,
    ) -> None:
        self._generator = generator
        self._validator = validator
        self._executor = executor
        self._max_retries = max_retries

    def run(
        self,
        messages: list[dict[str, str]],
    ) -> PipelineResult:
        """Run the pipeline, self-correcting on execution errors.

        Args:
            messages: Initial LLM messages (schema context + question).

        Returns:
            PipelineResult with the successful SQL, DataFrame, timing,
            attempt count, and full correction history.

        Raises:
            SQLValidationError: Unsafe SQL detected — never retried.
            PipelineError:      All attempts exhausted.
        """
        t0 = time.perf_counter()
        history: list[CorrectionStep] = []
        current_messages = list(messages)

        max_attempts = self._max_retries + 1

        for attempt in range(1, max_attempts + 1):

            # ── 1. Generate ────────────────────────────────────────────────
            try:
                sql = self._generator.generate(current_messages)
            except SQLGenerationError as exc:
                raise PipelineError(f"SQL generation failed: {exc}") from exc

            # ── 2. Safety validation — never retry ─────────────────────────
            self._validator.validate(sql)   # raises SQLValidationError immediately

            # ── 3. Execute ────────────────────────────────────────────────
            try:
                df = self._executor.execute(sql)
                elapsed = (time.perf_counter() - t0) * 1000

                if attempt > 1:
                    logger.info(
                        "Self-correction succeeded on attempt %d "
                        "(after %d correction(s))",
                        attempt, attempt - 1,
                    )

                return PipelineResult(
                    sql=sql,
                    df=df,
                    elapsed_ms=elapsed,
                    attempts=attempt,
                    correction_history=history,
                )

            except SQLExecutionError as exc:
                error_msg = str(exc)
                logger.warning(
                    "Attempt %d/%d failed — %s",
                    attempt, max_attempts, error_msg[:120],
                )
                history.append(CorrectionStep(attempt=attempt, sql=sql, error=error_msg))

                if attempt == max_attempts:
                    break

                # ── 4. Build correction context for the next attempt ───────
                current_messages = current_messages + [
                    {"role": "assistant", "content": f"```sql\n{sql}\n```"},
                    {"role": "user", "content": _CORRECTION_TEMPLATE.format(
                        sql=sql, error=error_msg,
                    )},
                ]

        elapsed = (time.perf_counter() - t0) * 1000
        last_error = history[-1].error if history else "unknown"
        last_sql = history[-1].sql if history else None
        raise PipelineError(
            f"All {max_attempts} attempt(s) exhausted. Last error: {last_error}",
            last_sql=last_sql,
        )
