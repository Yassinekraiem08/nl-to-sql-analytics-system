"""Tests for the execution-accuracy evaluator."""
from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from app.core.evaluator import (
    ExecutionEvaluator,
    EvalResult,
    classify_hardness,
    _normalize,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def engine():
    """In-memory SQLite with a tiny sales schema."""
    eng = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with eng.begin() as conn:
        conn.execute(text("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                city TEXT
            )
        """))
        conn.execute(text("""
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                amount REAL,
                status TEXT
            )
        """))
        conn.execute(text("INSERT INTO users VALUES (1,'Alice','NY'), (2,'Bob','LA'), (3,'Carol','NY')"))
        conn.execute(text("INSERT INTO orders VALUES (1,1,100.0,'completed'), (2,1,200.0,'completed'), (3,2,50.0,'pending')"))
    return eng


@pytest.fixture()
def evaluator():
    return ExecutionEvaluator()


# ---------------------------------------------------------------------------
# _normalize
# ---------------------------------------------------------------------------

def test_normalize_empty_df():
    assert _normalize(pd.DataFrame()) == frozenset()


def test_normalize_none():
    assert _normalize(None) == frozenset()


def test_normalize_order_insensitive():
    df1 = pd.DataFrame({"a": [1, 2, 3]})
    df2 = pd.DataFrame({"a": [3, 1, 2]})
    assert _normalize(df1) == _normalize(df2)


def test_normalize_float_rounding():
    df1 = pd.DataFrame({"v": [1.0000]})
    df2 = pd.DataFrame({"v": [1.00001]})   # within rounding tolerance
    assert _normalize(df1) == _normalize(df2)


def test_normalize_different_values():
    df1 = pd.DataFrame({"a": [1, 2]})
    df2 = pd.DataFrame({"a": [1, 99]})
    assert _normalize(df1) != _normalize(df2)


# ---------------------------------------------------------------------------
# ExecutionEvaluator.evaluate_pair
# ---------------------------------------------------------------------------

def test_matching_queries(engine, evaluator):
    gold = "SELECT COUNT(*) FROM users"
    pred = "SELECT COUNT(*) AS cnt FROM users"
    match, err = evaluator.evaluate_pair(gold, pred, engine)
    assert match is True
    assert err is None


def test_mismatched_results(engine, evaluator):
    # users=3 rows, completed orders=2 rows → different counts
    gold = "SELECT COUNT(*) FROM users"
    pred = "SELECT COUNT(*) FROM orders WHERE status = 'completed'"
    match, err = evaluator.evaluate_pair(gold, pred, engine)
    assert match is False


def test_pred_sql_error_returns_false(engine, evaluator):
    gold = "SELECT COUNT(*) FROM users"
    pred = "SELECT COUNT(*) FROM nonexistent_table"
    match, err = evaluator.evaluate_pair(gold, pred, engine)
    assert match is False
    assert err is not None
    assert "Pred SQL error" in err


def test_gold_sql_error_returns_false(engine, evaluator):
    gold = "SELECT * FROM missing"
    pred = "SELECT COUNT(*) FROM users"
    match, err = evaluator.evaluate_pair(gold, pred, engine)
    assert match is False
    assert "Gold SQL error" in err


def test_order_insensitive_match(engine, evaluator):
    gold = "SELECT id FROM users ORDER BY id ASC"
    pred = "SELECT id FROM users ORDER BY id DESC"
    # Both contain {1,2,3} — order shouldn't matter
    match, err = evaluator.evaluate_pair(gold, pred, engine)
    assert match is True


def test_join_query_match(engine, evaluator):
    gold = (
        "SELECT u.name, SUM(o.amount) AS total "
        "FROM users u JOIN orders o ON u.id = o.user_id "
        "GROUP BY u.id ORDER BY total DESC"
    )
    pred = (
        "SELECT u.name, SUM(o.amount) AS spend "
        "FROM orders o JOIN users u ON o.user_id = u.id "
        "GROUP BY u.name ORDER BY spend ASC"
    )
    match, _ = evaluator.evaluate_pair(gold, pred, engine)
    assert match is True


# ---------------------------------------------------------------------------
# ExecutionEvaluator.accuracy
# ---------------------------------------------------------------------------

def _make_result(match: bool, hardness: str = "easy", latency: float = 100.0) -> EvalResult:
    return EvalResult(
        question="q", db_id="db", gold_sql="SELECT 1", pred_sql="SELECT 1",
        execution_match=match, error=None, latency_ms=latency, hardness=hardness,
    )


def test_accuracy_empty():
    assert ExecutionEvaluator().accuracy([]) == {}


def test_accuracy_all_correct():
    results = [_make_result(True) for _ in range(10)]
    metrics = ExecutionEvaluator().accuracy(results)
    assert metrics["execution_accuracy"] == 1.0
    assert metrics["error_rate"] == 0.0


def test_accuracy_partial():
    results = [_make_result(True)] * 7 + [_make_result(False)] * 3
    metrics = ExecutionEvaluator().accuracy(results)
    assert metrics["execution_accuracy"] == pytest.approx(0.7)
    assert metrics["total"] == 10
    assert metrics["matched"] == 7


def test_accuracy_by_hardness():
    results = [
        _make_result(True, "easy"),
        _make_result(True, "easy"),
        _make_result(False, "hard"),
    ]
    metrics = ExecutionEvaluator().accuracy(results)
    assert metrics["by_hardness"]["easy"] == 1.0
    assert metrics["by_hardness"]["hard"] == 0.0


def test_accuracy_latency_percentiles():
    # latencies sorted: [10,20,30,40,50,60,70,80,90,100], n=10
    # p50 = latencies[(10-1)//2] = latencies[4] = 50.0
    # p95 = latencies[int((10-1)*0.95)] = latencies[8] = 90.0
    results = [_make_result(True, latency=float(i * 10)) for i in range(1, 11)]
    metrics = ExecutionEvaluator().accuracy(results)
    assert metrics["latency_p50_ms"] == 50.0
    assert metrics["latency_p95_ms"] == 90.0


# ---------------------------------------------------------------------------
# classify_hardness
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("sql,expected", [
    ("SELECT COUNT(*) FROM users", "easy"),
    ("SELECT * FROM users WHERE city = 'NY'", "easy"),
    ("SELECT u.name, COUNT(o.id) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.id", "hard"),
    ("SELECT city, COUNT(*) FROM users GROUP BY city HAVING COUNT(*) > 1 ORDER BY 2 DESC", "hard"),
    ("SELECT name FROM users INTERSECT SELECT name FROM orders", "extra_hard"),
    (
        "SELECT x FROM (SELECT id AS x FROM users WHERE id IN (SELECT user_id FROM orders)) sub",
        "extra_hard",
    ),
])
def test_classify_hardness(sql, expected):
    assert classify_hardness(sql) == expected
