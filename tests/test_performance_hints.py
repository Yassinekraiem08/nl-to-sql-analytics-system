"""Tests for SQL query performance hints."""
from __future__ import annotations

import pytest

from app.core.performance_hints import analyze_performance, PerformanceHint


# ---------------------------------------------------------------------------
# Schema fixture
# ---------------------------------------------------------------------------

SCHEMA = {
    "users": {
        "columns": [
            {"name": "id", "type": "INTEGER"},
            {"name": "email", "type": "TEXT"},
            {"name": "city", "type": "TEXT"},
            {"name": "created_at", "type": "TIMESTAMP"},
        ],
        "primary_keys": ["id"],
        "foreign_keys": [],
    },
    "orders": {
        "columns": [
            {"name": "id", "type": "INTEGER"},
            {"name": "user_id", "type": "INTEGER"},
            {"name": "status", "type": "TEXT"},
            {"name": "total", "type": "REAL"},
            {"name": "created_at", "type": "TIMESTAMP"},
        ],
        "primary_keys": ["id"],
        "foreign_keys": [
            {"constrained_columns": ["user_id"], "referred_table": "users", "referred_columns": ["id"]},
        ],
    },
}


# ---------------------------------------------------------------------------
# No hints expected
# ---------------------------------------------------------------------------

def test_simple_select_no_hints():
    sql = "SELECT id, email FROM users"
    hints = analyze_performance(sql, SCHEMA)
    assert hints == []


def test_empty_schema_returns_no_hints():
    sql = "SELECT * FROM users WHERE users.email = 'x'"
    hints = analyze_performance(sql, {})
    assert hints == []


def test_pk_column_not_hinted():
    # users.id is a PK — already indexed
    sql = "SELECT * FROM users WHERE users.id = 1"
    hints = analyze_performance(sql, SCHEMA)
    assert not any(h.column == "id" for h in hints)


def test_fk_column_not_hinted():
    # orders.user_id has a FK constraint — treated as indexed
    sql = "SELECT * FROM orders JOIN users ON orders.user_id = users.id"
    hints = analyze_performance(sql, SCHEMA)
    assert not any(h.table == "orders" and h.column == "user_id" for h in hints)


# ---------------------------------------------------------------------------
# WHERE hints
# ---------------------------------------------------------------------------

def test_where_filter_suggests_index():
    sql = "SELECT * FROM users WHERE users.email = 'alice@example.com'"
    hints = analyze_performance(sql, SCHEMA)
    assert any(h.table == "users" and h.column == "email" for h in hints)


def test_where_reason_label():
    sql = "SELECT * FROM users WHERE users.city = 'NY'"
    hints = analyze_performance(sql, SCHEMA)
    matching = [h for h in hints if h.column == "city"]
    assert matching
    assert "WHERE filter" in matching[0].reason


def test_multiple_where_columns():
    sql = "SELECT * FROM orders WHERE orders.status = 'done' AND orders.total > 100"
    hints = analyze_performance(sql, SCHEMA)
    cols = {h.column for h in hints}
    assert "status" in cols
    assert "total" in cols


# ---------------------------------------------------------------------------
# JOIN hints
# ---------------------------------------------------------------------------

def test_join_condition_suggests_index_for_unindexed_col():
    sql = "SELECT * FROM users JOIN orders ON users.email = orders.status"
    hints = analyze_performance(sql, SCHEMA)
    # users.email is not a PK/FK so should get a hint
    assert any(h.table == "users" and h.column == "email" for h in hints)


def test_join_reason_label():
    sql = "SELECT * FROM users JOIN orders ON users.city = orders.status"
    hints = analyze_performance(sql, SCHEMA)
    matching = [h for h in hints if h.column == "city"]
    assert matching
    assert "JOIN" in matching[0].reason


# ---------------------------------------------------------------------------
# ORDER BY hints
# ---------------------------------------------------------------------------

def test_order_by_suggests_index():
    sql = "SELECT * FROM orders ORDER BY orders.created_at DESC"
    hints = analyze_performance(sql, SCHEMA)
    assert any(h.table == "orders" and h.column == "created_at" for h in hints)


def test_order_by_reason_label():
    sql = "SELECT * FROM users ORDER BY users.created_at"
    hints = analyze_performance(sql, SCHEMA)
    matching = [h for h in hints if h.column == "created_at"]
    assert matching
    assert "ORDER BY" in matching[0].reason


# ---------------------------------------------------------------------------
# Deduplication & non-existent columns
# ---------------------------------------------------------------------------

def test_deduplicated_hints():
    # Same column appears in WHERE and ORDER BY — should only appear once
    sql = "SELECT * FROM users WHERE users.email = 'x' ORDER BY users.email"
    hints = analyze_performance(sql, SCHEMA)
    email_hints = [h for h in hints if h.column == "email"]
    assert len(email_hints) == 1


def test_nonexistent_column_not_hinted():
    sql = "SELECT * FROM users WHERE users.ghost_col = 1"
    hints = analyze_performance(sql, SCHEMA)
    assert not any(h.column == "ghost_col" for h in hints)


# ---------------------------------------------------------------------------
# __str__ format
# ---------------------------------------------------------------------------

def test_hint_str_format():
    hint = PerformanceHint(table="users", column="email", reason="used in WHERE filter")
    s = str(hint)
    assert "users.email" in s
    assert "WHERE filter" in s
    assert "Consider" in s
