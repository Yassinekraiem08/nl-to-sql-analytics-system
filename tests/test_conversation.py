"""Tests for multi-turn conversation: SessionStore, ConversationSession, prompt context."""
from __future__ import annotations

import pytest

from app.core.conversation import (
    ConversationSession,
    ConversationTurn,
    SessionStore,
)
from app.core.prompt_builder import PromptBuilder


# ---------------------------------------------------------------------------
# ConversationTurn / ConversationSession
# ---------------------------------------------------------------------------

def _turn(q="How many users?", sql="SELECT COUNT(*) FROM users", cols=None, rows=10):
    return ConversationTurn(
        question=q, sql=sql,
        result_columns=cols or ["count(*)"],
        row_count=rows, summary="There are 10 users.",
    )


def test_session_starts_empty():
    s = ConversationSession(session_id="abc")
    assert s.last_turn is None
    assert s.context_block() == ""


def test_session_add_turn():
    s = ConversationSession(session_id="abc")
    s.add_turn(_turn())
    assert s.last_turn is not None
    assert s.last_turn.question == "How many users?"


def test_context_block_contains_previous_sql():
    s = ConversationSession(session_id="abc")
    s.add_turn(_turn(sql="SELECT COUNT(*) FROM users"))
    ctx = s.context_block()
    assert "SELECT COUNT(*) FROM users" in ctx


def test_context_block_contains_previous_question():
    s = ConversationSession(session_id="abc")
    s.add_turn(_turn(q="How many users are there?"))
    assert "How many users are there?" in s.context_block()


def test_context_block_contains_columns():
    s = ConversationSession(session_id="abc")
    s.add_turn(_turn(cols=["user_id", "name", "city"]))
    ctx = s.context_block()
    assert "user_id" in ctx
    assert "name" in ctx


def test_context_block_only_uses_last_turn():
    s = ConversationSession(session_id="abc")
    s.add_turn(_turn(q="First question", sql="SELECT 1"))
    s.add_turn(_turn(q="Second question", sql="SELECT 2"))
    ctx = s.context_block()
    assert "Second question" in ctx
    assert "SELECT 2" in ctx
    assert "First question" not in ctx


def test_session_multi_turn_history():
    s = ConversationSession(session_id="abc")
    for i in range(5):
        s.add_turn(_turn(q=f"Question {i}"))
    assert len(s.turns) == 5


# ---------------------------------------------------------------------------
# SessionStore
# ---------------------------------------------------------------------------

def test_store_create_returns_session():
    store = SessionStore()
    s = store.create()
    assert s.session_id
    assert len(s.session_id) == 36  # UUID format


def test_store_get_returns_same_session():
    store = SessionStore()
    s = store.create()
    assert store.get(s.session_id) is s


def test_store_get_unknown_returns_none():
    store = SessionStore()
    assert store.get("nonexistent-id") is None


def test_store_delete_returns_true():
    store = SessionStore()
    s = store.create()
    assert store.delete(s.session_id) is True
    assert store.get(s.session_id) is None


def test_store_delete_unknown_returns_false():
    store = SessionStore()
    assert store.delete("ghost") is False


def test_store_active_count():
    store = SessionStore()
    store.create()
    store.create()
    assert store.active_count == 2


def test_store_database_url_forwarded():
    store = SessionStore()
    s = store.create(database_url="sqlite:///custom.db")
    assert s.database_url == "sqlite:///custom.db"


# ---------------------------------------------------------------------------
# PromptBuilder conversation context injection
# ---------------------------------------------------------------------------

SCHEMA = {
    "users": {
        "columns": [{"name": "id", "type": "INTEGER"}, {"name": "city", "type": "TEXT"}],
        "primary_keys": ["id"], "foreign_keys": [], "sample_rows": [],
    }
}


def test_prompt_has_no_context_on_first_turn():
    builder = PromptBuilder(SCHEMA)
    messages = builder.build("How many users?")
    assert "Previous question" not in messages[0]["content"]


def test_prompt_injects_context_on_followup():
    builder = PromptBuilder(SCHEMA)
    ctx = (
        "Previous question: How many users?\n"
        "Previous SQL:\n```sql\nSELECT COUNT(*) FROM users\n```\n"
        "Previous result columns: count(*)\n"
        "Previous row count: 1\n"
        "\nThe user is asking a follow-up."
    )
    messages = builder.build("Now filter by New York", conversation_context=ctx)
    content = messages[0]["content"]
    assert "Previous question" in content
    assert "SELECT COUNT(*) FROM users" in content


def test_prompt_context_appears_before_current_question():
    builder = PromptBuilder(SCHEMA)
    ctx = "Previous question: Q1\nPrevious SQL:\n```sql\nSELECT 1\n```\n\nFoo"
    messages = builder.build("Follow up", conversation_context=ctx)
    content = messages[0]["content"]
    assert content.index("Previous question") < content.index("Question: Follow up")


# ---------------------------------------------------------------------------
# Session endpoint integration
# ---------------------------------------------------------------------------

from fastapi.testclient import TestClient
from unittest.mock import patch
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool
from app.api.main import app


@pytest.fixture()
def client():
    eng = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, city TEXT)"))
        conn.execute(text("INSERT INTO users VALUES (1,'Alice','NY'),(2,'Bob','LA')"))

    with patch("app.db.connection.get_engine", return_value=eng), \
         patch("app.api.routes.query.get_engine", return_value=eng):
        with TestClient(app) as c:
            yield c


def test_create_session_endpoint(client):
    res = client.post("/sessions", json={})
    assert res.status_code == 201
    data = res.json()
    assert "session_id" in data
    assert data["turn_count"] == 0


def test_get_session_history_empty(client):
    sid = client.post("/sessions", json={}).json()["session_id"]
    res = client.get(f"/sessions/{sid}")
    assert res.status_code == 200
    assert res.json()["turns"] == []


def test_get_unknown_session_returns_404(client):
    res = client.get("/sessions/nonexistent")
    assert res.status_code == 404


def test_delete_session(client):
    sid = client.post("/sessions", json={}).json()["session_id"]
    assert client.delete(f"/sessions/{sid}").status_code == 204
    assert client.get(f"/sessions/{sid}").status_code == 404
