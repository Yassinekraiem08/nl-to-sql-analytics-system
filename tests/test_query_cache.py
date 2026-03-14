"""Tests for the semantic query cache."""
from __future__ import annotations

import pytest
from pathlib import Path

from app.core.query_cache import QueryCache, CachedResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _payload(question: str = "How many users?") -> dict:
    return {
        "question": question, "sql": "SELECT COUNT(*) FROM users",
        "rows": [{"count": 42}], "row_count": 1, "summary": "42 users.",
        "chart": None, "execution_time_ms": 12.0,
        "trace": {
            "tables_selected": ["users"], "relationships_used": [],
            "schema_issues": [], "attempts": 1, "correction_history": [],
        },
        "confidence": 0.95, "cache_hit": False,
        "performance_hints": [], "ambiguity_warning": None,
    }


def _cache(tmp_path: Path, threshold: float = 0.92) -> QueryCache:
    return QueryCache(tmp_path / "cache", threshold=threshold)


# ---------------------------------------------------------------------------
# Basic lookup / store
# ---------------------------------------------------------------------------

def test_empty_cache_returns_none(tmp_path):
    c = _cache(tmp_path)
    assert c.lookup("Any question?") is None


def test_store_and_lookup_exact_match(tmp_path):
    c = _cache(tmp_path)
    c.store("How many users?", _payload(), db_id="demo")
    hit = c.lookup("How many users?", db_id="demo")
    assert hit is not None
    assert hit.question == "How many users?"


def test_lookup_similar_question_above_threshold(tmp_path):
    c = _cache(tmp_path, threshold=0.7)
    c.store("How many users are there?", _payload(), db_id="demo")
    # Same question slightly rephrased — should still match at threshold 0.7
    hit = c.lookup("How many users are there?", db_id="demo")
    assert hit is not None


def test_lookup_dissimilar_question_returns_none(tmp_path):
    c = _cache(tmp_path)
    c.store("How many users?", _payload(), db_id="demo")
    hit = c.lookup("What is the total revenue by category?", db_id="demo")
    assert hit is None


def test_threshold_zero_always_hits(tmp_path):
    c = _cache(tmp_path, threshold=0.0)
    c.store("How many users?", _payload(), db_id="demo")
    hit = c.lookup("Completely unrelated question xyz", db_id="demo")
    assert hit is not None


def test_threshold_one_never_hits_dissimilar(tmp_path):
    c = _cache(tmp_path, threshold=1.0)
    c.store("How many users?", _payload(), db_id="demo")
    hit = c.lookup("What products have the highest margin?", db_id="demo")
    assert hit is None


# ---------------------------------------------------------------------------
# Cache size & clear
# ---------------------------------------------------------------------------

def test_cache_size_tracking(tmp_path):
    c = _cache(tmp_path)
    assert c.size == 0
    c.store("Q1", _payload(), db_id="demo")
    c.store("Q2", _payload(), db_id="demo")
    assert c.size == 2


def test_clear_empties_cache(tmp_path):
    c = _cache(tmp_path)
    c.store("How many users?", _payload(), db_id="demo")
    c.clear()
    assert c.size == 0
    assert c.lookup("How many users?", db_id="demo") is None


# ---------------------------------------------------------------------------
# db_id isolation
# ---------------------------------------------------------------------------

def test_db_id_isolation(tmp_path):
    c = _cache(tmp_path)
    c.store("How many users?", _payload(), db_id="db_a")
    assert c.lookup("How many users?", db_id="db_b") is None


def test_lookup_correct_db_id(tmp_path):
    c = _cache(tmp_path)
    c.store("How many users?", _payload(), db_id="db_a")
    hit = c.lookup("How many users?", db_id="db_a")
    assert hit is not None


# ---------------------------------------------------------------------------
# Payload integrity
# ---------------------------------------------------------------------------

def test_cache_hit_payload_preserved(tmp_path):
    payload = _payload()
    payload["row_count"] = 99
    c = _cache(tmp_path)
    c.store("How many users?", payload, db_id="demo")
    hit = c.lookup("How many users?", db_id="demo")
    assert hit is not None
    assert hit.payload["row_count"] == 99


def test_multiple_entries_returns_best_match(tmp_path):
    c = _cache(tmp_path, threshold=0.5)
    p1 = _payload("Count users")
    p1["row_count"] = 1
    p2 = _payload("Total revenue")
    p2["row_count"] = 2
    c.store("Count users", p1, db_id="demo")
    c.store("Total revenue by product", p2, db_id="demo")
    hit = c.lookup("Count users please", db_id="demo")
    assert hit is not None
    assert hit.payload["row_count"] == 1


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def test_persistence_round_trip(tmp_path):
    c1 = _cache(tmp_path)
    c1.store("How many users?", _payload(), db_id="demo")

    c2 = QueryCache(tmp_path / "cache")
    assert c2.size == 1
    hit = c2.lookup("How many users?", db_id="demo")
    assert hit is not None


def test_load_from_disk_preserves_threshold(tmp_path):
    c = _cache(tmp_path, threshold=0.5)
    c.store("How many users?", _payload(), db_id="demo")
    # Re-load with same path
    c2 = QueryCache(tmp_path / "cache", threshold=0.5)
    assert c2.size == 1
