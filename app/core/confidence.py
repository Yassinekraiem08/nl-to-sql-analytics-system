"""Confidence scoring for generated SQL queries.

Produces a 0–1 score that reflects how trustworthy a response is, based on:
  - Schema validation issues (hallucinated tables/columns)
  - Number of self-correction attempts needed
  - Whether the query returned any rows

The score is a heuristic, not a probability — but it correlates well with
execution accuracy in practice and gives users a fast visual signal.
"""
from __future__ import annotations

from typing import Any


def compute_confidence(
    schema_issues: list[Any],   # list of SchemaIssue objects (have .severity attr)
    attempts: int,
    row_count: int,
) -> float:
    """Return a confidence score in [0.0, 1.0].

    Deductions:
      - Each schema error   → −0.20  (hallucinated table/column)
      - Each schema warning → −0.08  (fuzzy column match)
      - Each correction     → −0.15  (LLM needed a retry)
      - Empty result set    → −0.08  (might indicate wrong query intent)
    """
    score = 1.0

    for issue in schema_issues:
        severity = getattr(issue, "severity", "warning")
        if severity == "error":
            score -= 0.20
        else:
            score -= 0.08

    # Correction penalty (attempts=1 means first-try success)
    score -= max(0, attempts - 1) * 0.15

    if row_count == 0:
        score -= 0.08

    return round(max(0.0, min(1.0, score)), 4)


def confidence_label(score: float) -> str:
    """Human-readable tier for display."""
    if score >= 0.90:
        return "high"
    if score >= 0.75:
        return "medium"
    if score >= 0.60:
        return "low"
    return "very_low"
