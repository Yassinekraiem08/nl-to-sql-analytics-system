"""Ambiguity detection for natural-language questions.

Flags questions that contain vague temporal, quantitative, or qualitative
terms so the API can surface a warning and suggest clarifications.

Examples of ambiguous questions:
    "Show me recent orders"   → recent = last day? week? month?
    "Find some large orders"  → large = > $100? > $1000?
    "Who are the top users?"  → top by what metric?
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# Vague term catalogue
# ---------------------------------------------------------------------------

_TEMPORAL_VAGUE = {
    "recent", "recently", "latest", "last", "new", "current", "old", "outdated",
}
_QUANTITY_VAGUE = {
    "some", "few", "many", "several", "most", "least", "lot", "lots",
}
_QUALITY_VAGUE = {
    "top", "best", "worst", "good", "bad", "high", "low", "large", "small",
    "big", "popular", "important", "significant",
}

_ALL_VAGUE = _TEMPORAL_VAGUE | _QUANTITY_VAGUE | _QUALITY_VAGUE

_SUGGESTIONS: dict[str, str] = {
    "recent":      "Specify a time range, e.g. 'in the last 7 days' or 'this month'",
    "recently":    "Specify a time range, e.g. 'in the last 7 days' or 'this month'",
    "latest":      "Specify a time range, e.g. 'in the last 7 days' or 'this month'",
    "last":        "Specify a time range, e.g. 'in the last 7 days'",
    "new":         "Clarify what 'new' means, e.g. 'created after 2024-01-01'",
    "current":     "Specify a time range, e.g. 'this month' or 'this year'",
    "old":         "Clarify 'old', e.g. 'created before 2023-01-01'",
    "outdated":    "Clarify 'outdated', e.g. 'not updated in 30 days'",
    "some":        "Specify a quantity, e.g. 'top 10' or 'at least 5'",
    "few":         "Specify a number, e.g. 'fewer than 5'",
    "many":        "Specify a threshold, e.g. 'more than 100'",
    "several":     "Specify a number, e.g. 'at least 3'",
    "most":        "Clarify 'most', e.g. 'top 10 by count' or 'more than 50%'",
    "least":       "Clarify 'least', e.g. 'bottom 5 by revenue'",
    "lot":         "Specify a quantity, e.g. 'more than 50'",
    "lots":        "Specify a quantity, e.g. 'more than 50'",
    "top":         "Specify how many and by what metric, e.g. 'top 10 by revenue'",
    "best":        "Specify the metric, e.g. 'highest revenue' or 'most orders'",
    "worst":       "Specify the metric, e.g. 'lowest revenue' or 'fewest orders'",
    "good":        "Clarify 'good', e.g. 'rating above 4' or 'revenue > $1000'",
    "bad":         "Clarify 'bad', e.g. 'rating below 2' or 'revenue < $100'",
    "high":        "Specify a threshold, e.g. 'revenue > $1000' or 'score > 90'",
    "low":         "Specify a threshold, e.g. 'revenue < $100' or 'score < 50'",
    "large":       "Specify a size, e.g. 'quantity > 100' or 'order total > $500'",
    "small":       "Specify a size, e.g. 'quantity < 10' or 'order total < $50'",
    "big":         "Specify a size, e.g. 'more than 100 items' or 'revenue > $1000'",
    "popular":     "Specify what 'popular' means, e.g. 'ordered more than 50 times'",
    "important":   "Specify what 'important' means, e.g. 'revenue > $10000'",
    "significant": "Specify what 'significant' means, e.g. 'change > 10%'",
}


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------

@dataclass
class AmbiguityResult:
    is_ambiguous: bool
    vague_terms: list[str] = field(default_factory=list)
    suggestions: list[str] = field(default_factory=list)

    def warning_text(self) -> str:
        """Single-string summary suitable for embedding in an API response."""
        if not self.is_ambiguous:
            return ""
        return "Ambiguous terms detected — " + "; ".join(self.suggestions)


# ---------------------------------------------------------------------------
# Detector
# ---------------------------------------------------------------------------

def detect_ambiguity(question: str) -> AmbiguityResult:
    """Return an AmbiguityResult for the given natural-language question.

    Only flags terms that appear as whole words (not inside longer words),
    so "latest" is flagged but "latency" is not.
    """
    words = set(re.findall(r"\b\w+\b", question.lower()))
    found = sorted(words & _ALL_VAGUE)

    if not found:
        return AmbiguityResult(is_ambiguous=False)

    suggestions = [
        f"'{term}' — {_SUGGESTIONS[term]}"
        for term in found
        if term in _SUGGESTIONS
    ]

    return AmbiguityResult(
        is_ambiguous=True,
        vague_terms=found,
        suggestions=suggestions,
    )
