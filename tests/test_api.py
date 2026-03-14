"""Integration tests for the FastAPI endpoints using SQLite + mocked LLM."""
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

# Point config at an in-memory SQLite before importing the app
import os
os.environ.setdefault("LLM_PROVIDER", "openai")
os.environ.setdefault("OPENAI_API_KEY", "sk-test")
os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")

from app.api.main import app
from app.db.connection import get_engine


# ── Fixtures

@pytest.fixture(scope="module")
def sqlite_engine():
    # StaticPool ensures all connections share the same underlying SQLite
    # connection, so the in-memory database persists across multiple connects.
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"))
        conn.execute(text("INSERT INTO users VALUES (1, 'Alice', 30)"))
        conn.execute(text("INSERT INTO users VALUES (2, 'Bob', 25)"))
        conn.commit()
    return engine


@pytest.fixture(scope="module")
def client(sqlite_engine):
    """Patch get_engine in every route module so they all use the test DB."""
    import app.api.routes.health as health_mod
    import app.api.routes.schema as schema_mod
    import app.api.routes.query as query_mod

    _engine_fn = lambda: sqlite_engine  # noqa: E731

    original_health = health_mod.get_engine
    original_schema = schema_mod.get_engine
    original_query = query_mod.get_engine

    health_mod.get_engine = _engine_fn
    schema_mod.get_engine = _engine_fn
    query_mod.get_engine = _engine_fn

    with TestClient(app) as c:
        yield c

    health_mod.get_engine = original_health
    schema_mod.get_engine = original_schema
    query_mod.get_engine = original_query


# ── Tests

def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"


def test_schema_full(client):
    resp = client.get("/schema")
    assert resp.status_code == 200
    assert "users" in resp.json()["tables"]


def test_schema_table(client):
    resp = client.get("/schema/users")
    assert resp.status_code == 200
    col_names = [c["name"] for c in resp.json()["columns"]]
    assert "id" in col_names


def test_schema_missing_table(client):
    resp = client.get("/schema/does_not_exist")
    assert resp.status_code == 404


@patch("app.core.llm_client.LLMClient.complete")
def test_ask_endpoint(mock_complete, client):
    def side_effect(messages, *, system_prompt=None, max_tokens=1024):
        if system_prompt:  # SQL generation always passes a system_prompt
            return "```sql\nSELECT * FROM users\n```"
        return "There are 2 users."

    mock_complete.side_effect = side_effect

    resp = client.post("/ask", json={"question": "Show all users"})
    assert resp.status_code == 200
    data = resp.json()
    assert "sql" in data
    assert data["row_count"] == 2
    assert "summary" in data
    assert "trace" in data
    assert "tables_selected" in data["trace"]
