"""Execution-accuracy evaluator for NL-to-SQL pipelines.

Execution accuracy is the primary metric: run both gold and predicted SQL
on the same database, compare result sets order-insensitively. This is more
meaningful than exact string match because many SQL queries are semantically
equivalent but syntactically different.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd
from sqlalchemy import Engine, text
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Hardness classification (approximates Spider's official categorisation)
# ---------------------------------------------------------------------------

def classify_hardness(sql: str) -> str:
    """Classify SQL difficulty based on structural complexity.

    Scoring (calibrated to approximate Spider's official categorisation):
      JOIN        → 1 point each
      GROUP BY    → 3 points  (aggregation is the main difficulty driver)
      HAVING      → 1 point
      SET ops     → 6 points  (INTERSECT / UNION / EXCEPT)
      subqueries  → 3 points per extra SELECT beyond the first
    Thresholds: easy ≤ 0 | medium ≤ 2 | hard ≤ 5 | extra_hard > 5
    """
    u = sql.upper()
    score = 0
    score += u.count("JOIN")
    score += 3 if "GROUP BY" in u else 0
    score += 1 if "HAVING" in u else 0
    score += 6 if any(k in u for k in ("INTERSECT", "UNION", "EXCEPT")) else 0
    score += 3 * (u.count("SELECT") - 1)   # nested SELECT = subquery
    if score == 0:
        return "easy"
    if score <= 2:
        return "medium"
    if score <= 5:
        return "hard"
    return "extra_hard"


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class EvalResult:
    question: str
    db_id: str
    gold_sql: str
    pred_sql: Optional[str]
    execution_match: bool
    error: Optional[str]
    latency_ms: float
    hardness: str = "unknown"
    retry_count: int = 0


# ---------------------------------------------------------------------------
# Core evaluator
# ---------------------------------------------------------------------------

def _normalize(df: pd.DataFrame) -> frozenset:
    """Convert a DataFrame to an order-insensitive frozenset of row tuples.

    Floats are rounded to 4 decimal places to absorb minor precision differences
    between equivalent expressions (e.g. SUM vs manual addition).
    """
    if df is None or df.empty:
        return frozenset()

    def _round(x: object) -> object:
        return round(float(x), 4) if isinstance(x, (float, int)) else x

    rounded = df.apply(lambda col: col.map(_round))
    return frozenset(map(tuple, rounded.values.tolist()))


class ExecutionEvaluator:
    """Compare gold vs predicted SQL by executing both and diffing result sets."""

    def evaluate_pair(
        self,
        gold_sql: str,
        pred_sql: str,
        engine: Engine,
    ) -> tuple[bool, Optional[str]]:
        """Execute both queries; return (match, error_msg).

        If gold SQL itself errors, the example is skipped (returns False + error).
        """
        try:
            with engine.connect() as conn:
                gold_df = pd.read_sql(text(gold_sql), conn)
        except (SQLAlchemyError, Exception) as exc:
            return False, f"Gold SQL error: {exc}"

        try:
            with engine.connect() as conn:
                pred_df = pd.read_sql(text(pred_sql), conn)
        except (SQLAlchemyError, Exception) as exc:
            return False, f"Pred SQL error: {exc}"

        return _normalize(gold_df) == _normalize(pred_df), None

    # ------------------------------------------------------------------
    # Aggregate metrics
    # ------------------------------------------------------------------

    def accuracy(self, results: list[EvalResult]) -> dict:
        """Compute aggregate metrics from a completed evaluation run."""
        total = len(results)
        if total == 0:
            return {}

        matched = sum(1 for r in results if r.execution_match)
        errored = sum(1 for r in results if r.error)
        retried = sum(1 for r in results if r.retry_count > 0)

        # Per-hardness breakdown
        by_hardness: dict[str, dict[str, int]] = {}
        for r in results:
            h = r.hardness
            if h not in by_hardness:
                by_hardness[h] = {"total": 0, "matched": 0}
            by_hardness[h]["total"] += 1
            if r.execution_match:
                by_hardness[h]["matched"] += 1

        hardness_acc = {
            h: round(v["matched"] / v["total"], 4)
            for h, v in by_hardness.items()
        }

        latencies = sorted(r.latency_ms for r in results)
        n = len(latencies)
        # Use (n-1)-based indexing: standard nearest-rank percentile
        p50 = latencies[(n - 1) // 2]
        p95 = latencies[min(int((n - 1) * 0.95), n - 1)]
        return {
            "total": total,
            "execution_accuracy": round(matched / total, 4),
            "matched": matched,
            "error_rate": round(errored / total, 4),
            "retry_rate": round(retried / total, 4),
            "by_hardness": hardness_acc,
            "latency_p50_ms": p50,
            "latency_p95_ms": p95,
            "latency_mean_ms": round(sum(latencies) / n, 1),
        }
