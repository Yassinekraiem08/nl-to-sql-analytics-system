"""Tests for the confidence scoring function and schema graph endpoint."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock

from app.core.confidence import compute_confidence, confidence_label


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _issue(severity: str):
    m = MagicMock()
    m.severity = severity
    return m


# ---------------------------------------------------------------------------
# compute_confidence
# ---------------------------------------------------------------------------

def test_perfect_query_scores_one():
    score = compute_confidence(schema_issues=[], attempts=1, row_count=10)
    assert score == 1.0


def test_empty_rows_deducts():
    score = compute_confidence(schema_issues=[], attempts=1, row_count=0)
    assert score < 1.0
    assert score == pytest.approx(1.0 - 0.08)


def test_schema_error_deducts_more_than_warning():
    s_err  = compute_confidence([_issue("error")],   attempts=1, row_count=1)
    s_warn = compute_confidence([_issue("warning")], attempts=1, row_count=1)
    assert s_err < s_warn


def test_each_error_deducts_020():
    score = compute_confidence([_issue("error")], attempts=1, row_count=1)
    assert score == pytest.approx(1.0 - 0.20)


def test_each_warning_deducts_008():
    score = compute_confidence([_issue("warning")], attempts=1, row_count=1)
    assert score == pytest.approx(1.0 - 0.08)


def test_correction_attempt_deducts_015_per_extra():
    s1 = compute_confidence([], attempts=1, row_count=1)
    s2 = compute_confidence([], attempts=2, row_count=1)
    s3 = compute_confidence([], attempts=3, row_count=1)
    assert s1 - s2 == pytest.approx(0.15)
    assert s1 - s3 == pytest.approx(0.30)


def test_score_never_below_zero():
    issues = [_issue("error")] * 10
    score = compute_confidence(issues, attempts=5, row_count=0)
    assert score == 0.0


def test_score_never_above_one():
    score = compute_confidence([], attempts=1, row_count=100)
    assert score <= 1.0


def test_combined_deductions():
    # 1 error (-0.20) + 1 correction (-0.15) + empty (-0.08) = 0.57
    score = compute_confidence([_issue("error")], attempts=2, row_count=0)
    assert score == pytest.approx(1.0 - 0.20 - 0.15 - 0.08)


# ---------------------------------------------------------------------------
# confidence_label
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("score,expected", [
    (1.0,  "high"),
    (0.92, "high"),
    (0.90, "high"),
    (0.89, "medium"),
    (0.75, "medium"),
    (0.74, "low"),
    (0.60, "low"),
    (0.59, "very_low"),
    (0.0,  "very_low"),
])
def test_confidence_label(score, expected):
    assert confidence_label(score) == expected


# ---------------------------------------------------------------------------
# Schema graph endpoint
# ---------------------------------------------------------------------------

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool
from unittest.mock import patch

from app.api.main import app


@pytest.fixture()
def demo_client(tmp_path):
    """TestClient with a real in-memory DB so /schema/graph works end-to-end."""
    eng = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"))
        conn.execute(text("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL)"))

    with patch("app.api.routes.schema.get_engine", return_value=eng), \
         patch("app.db.connection.get_engine", return_value=eng):
        with TestClient(app) as client:
            yield client


def test_schema_graph_returns_nodes(demo_client):
    res = demo_client.get("/schema/graph")
    assert res.status_code == 200
    data = res.json()
    table_ids = [n["id"] for n in data["nodes"]]
    assert "users" in table_ids
    assert "orders" in table_ids


def test_schema_graph_nodes_have_columns(demo_client):
    res = demo_client.get("/schema/graph")
    nodes = {n["id"]: n for n in res.json()["nodes"]}
    assert "name" in nodes["users"]["columns"]
    assert "user_id" in nodes["orders"]["columns"]


def test_schema_graph_detects_fk_edge(demo_client):
    res = demo_client.get("/schema/graph")
    edges = res.json()["edges"]
    assert len(edges) >= 1
    # orders.user_id → users.id  (heuristic)
    edge = next(
        (e for e in edges if e["from_table"] == "orders" and e["to_table"] == "users"),
        None,
    )
    assert edge is not None
    assert edge["from_col"] == "user_id"
