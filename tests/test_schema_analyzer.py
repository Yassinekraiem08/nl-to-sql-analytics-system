"""Tests for SchemaAnalyzer — relationship graph and row counts."""
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from app.core.schema_analyzer import SchemaAnalyzer, RelationshipGraph, JoinEdge


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _engine_with(*ddl_statements: str):
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with engine.connect() as conn:
        for ddl in ddl_statements:
            conn.execute(text(ddl))
        conn.commit()
    return engine


def _schema_no_fks():
    """Two unrelated tables — no FK constraints, no naming hints."""
    return {
        "users": {
            "columns": [
                {"name": "id", "type": "INTEGER", "nullable": False},
                {"name": "name", "type": "TEXT", "nullable": True},
            ],
            "primary_keys": ["id"],
            "foreign_keys": [],
            "sample_rows": [],
        },
        "products": {
            "columns": [
                {"name": "id", "type": "INTEGER", "nullable": False},
                {"name": "title", "type": "TEXT", "nullable": True},
            ],
            "primary_keys": ["id"],
            "foreign_keys": [],
            "sample_rows": [],
        },
    }


def _schema_explicit_fk():
    """orders.user_id → users.id declared as an explicit FK."""
    return {
        "users": {
            "columns": [
                {"name": "id", "type": "INTEGER", "nullable": False},
                {"name": "name", "type": "TEXT", "nullable": True},
            ],
            "primary_keys": ["id"],
            "foreign_keys": [],
            "sample_rows": [],
        },
        "orders": {
            "columns": [
                {"name": "id", "type": "INTEGER", "nullable": False},
                {"name": "user_id", "type": "INTEGER", "nullable": False},
                {"name": "total", "type": "REAL", "nullable": True},
            ],
            "primary_keys": ["id"],
            "foreign_keys": [
                {
                    "columns": ["user_id"],
                    "referred_table": "users",
                    "referred_columns": ["id"],
                }
            ],
            "sample_rows": [],
        },
    }


def _schema_heuristic_fk():
    """Same tables but FK is NOT declared — only the column name gives it away."""
    schema = _schema_explicit_fk()
    schema["orders"]["foreign_keys"] = []
    return schema


def _schema_multi_hop():
    """users → orders → order_items — two-level chain."""
    return {
        "users": {
            "columns": [{"name": "id", "type": "INTEGER", "nullable": False}],
            "primary_keys": ["id"],
            "foreign_keys": [],
            "sample_rows": [],
        },
        "orders": {
            "columns": [
                {"name": "id", "type": "INTEGER", "nullable": False},
                {"name": "user_id", "type": "INTEGER", "nullable": False},
            ],
            "primary_keys": ["id"],
            "foreign_keys": [
                {
                    "columns": ["user_id"],
                    "referred_table": "users",
                    "referred_columns": ["id"],
                }
            ],
            "sample_rows": [],
        },
        "order_items": {
            "columns": [
                {"name": "id", "type": "INTEGER", "nullable": False},
                {"name": "order_id", "type": "INTEGER", "nullable": False},
                {"name": "product_id", "type": "INTEGER", "nullable": False},
            ],
            "primary_keys": ["id"],
            "foreign_keys": [
                {
                    "columns": ["order_id"],
                    "referred_table": "orders",
                    "referred_columns": ["id"],
                }
            ],
            "sample_rows": [],
        },
        "products": {
            "columns": [{"name": "id", "type": "INTEGER", "nullable": False}],
            "primary_keys": ["id"],
            "foreign_keys": [],
            "sample_rows": [],
        },
    }


# ------------------------------------------------------------------
# RelationshipGraph unit tests
# ------------------------------------------------------------------

def test_graph_neighbors_bidirectional():
    graph = RelationshipGraph(edges=[
        JoinEdge("orders", "user_id", "users", "id", "explicit_fk")
    ])
    assert "users" in graph.neighbors("orders")
    assert "orders" in graph.neighbors("users")


def test_graph_no_neighbors_for_isolated_table():
    graph = RelationshipGraph(edges=[
        JoinEdge("orders", "user_id", "users", "id", "explicit_fk")
    ])
    assert graph.neighbors("products") == []


def test_graph_join_hint_forward():
    graph = RelationshipGraph(edges=[
        JoinEdge("orders", "user_id", "users", "id", "explicit_fk")
    ])
    assert graph.join_hint("orders", "users") == "orders.user_id = users.id"


def test_graph_join_hint_reverse():
    graph = RelationshipGraph(edges=[
        JoinEdge("orders", "user_id", "users", "id", "explicit_fk")
    ])
    assert graph.join_hint("users", "orders") == "orders.user_id = users.id"


def test_graph_join_hint_unrelated_tables():
    graph = RelationshipGraph(edges=[
        JoinEdge("orders", "user_id", "users", "id", "explicit_fk")
    ])
    assert graph.join_hint("users", "products") is None


def test_graph_render_no_edges():
    graph = RelationshipGraph()
    assert "no relationships" in graph.render()


def test_graph_render_shows_edges():
    graph = RelationshipGraph(edges=[
        JoinEdge("orders", "user_id", "users", "id", "explicit_fk"),
        JoinEdge("order_items", "order_id", "orders", "id", "heuristic"),
    ])
    rendered = graph.render()
    assert "orders.user_id" in rendered
    assert "order_items.order_id" in rendered


# ------------------------------------------------------------------
# SchemaAnalyzer.build_graph
# ------------------------------------------------------------------

def test_no_fks_no_naming_hints_yields_empty_graph():
    engine = _engine_with(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
        "CREATE TABLE products (id INTEGER PRIMARY KEY, title TEXT)",
    )
    analyzer = SchemaAnalyzer(_schema_no_fks(), engine)
    graph = analyzer.build_graph()
    assert graph.edges == []


def test_explicit_fk_detected():
    engine = _engine_with(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL, FOREIGN KEY(user_id) REFERENCES users(id))",
    )
    analyzer = SchemaAnalyzer(_schema_explicit_fk(), engine)
    graph = analyzer.build_graph()

    explicit = [e for e in graph.edges if e.source == "explicit_fk"]
    assert len(explicit) == 1
    assert explicit[0].from_table == "orders"
    assert explicit[0].from_col == "user_id"
    assert explicit[0].to_table == "users"
    assert explicit[0].to_col == "id"


def test_heuristic_fk_detected_when_no_constraint():
    engine = _engine_with(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL)",
    )
    analyzer = SchemaAnalyzer(_schema_heuristic_fk(), engine)
    graph = analyzer.build_graph()

    heuristic = [e for e in graph.edges if e.source == "heuristic"]
    assert len(heuristic) == 1
    assert heuristic[0].from_table == "orders"
    assert heuristic[0].from_col == "user_id"
    assert heuristic[0].to_table == "users"


def test_no_duplicate_edges_when_explicit_and_naming_both_match():
    """Explicit FK should prevent a duplicate heuristic edge for the same column."""
    engine = _engine_with(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, FOREIGN KEY(user_id) REFERENCES users(id))",
    )
    analyzer = SchemaAnalyzer(_schema_explicit_fk(), engine)
    graph = analyzer.build_graph()

    edges_for_user_id = [
        e for e in graph.edges if e.from_table == "orders" and e.from_col == "user_id"
    ]
    assert len(edges_for_user_id) == 1


def test_multi_hop_chain_detected():
    engine = _engine_with(
        "CREATE TABLE users (id INTEGER PRIMARY KEY)",
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, FOREIGN KEY(user_id) REFERENCES users(id))",
        "CREATE TABLE order_items (id INTEGER PRIMARY KEY, order_id INTEGER, product_id INTEGER, FOREIGN KEY(order_id) REFERENCES orders(id))",
        "CREATE TABLE products (id INTEGER PRIMARY KEY)",
    )
    analyzer = SchemaAnalyzer(_schema_multi_hop(), engine)
    graph = analyzer.build_graph()

    tables_with_edges = {e.from_table for e in graph.edges} | {e.to_table for e in graph.edges}
    assert "users" in tables_with_edges
    assert "orders" in tables_with_edges
    assert "order_items" in tables_with_edges

    # order_items neighbors: orders (explicit FK) + products (heuristic via product_id)
    neighbors = graph.neighbors("order_items")
    assert "orders" in neighbors


# ------------------------------------------------------------------
# SchemaAnalyzer.row_counts
# ------------------------------------------------------------------

def test_row_counts_match_actual_data():
    engine = _engine_with(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
        "INSERT INTO users VALUES (1, 'Alice')",
        "INSERT INTO users VALUES (2, 'Bob')",
        "INSERT INTO users VALUES (3, 'Carol')",
    )
    schema = {
        "users": {
            "columns": [{"name": "id", "type": "INTEGER", "nullable": False}],
            "primary_keys": ["id"],
            "foreign_keys": [],
            "sample_rows": [],
        }
    }
    analyzer = SchemaAnalyzer(schema, engine)
    counts = analyzer.row_counts()
    assert counts["users"] == 3


def test_row_counts_empty_table_is_zero():
    engine = _engine_with("CREATE TABLE empty_tbl (id INTEGER PRIMARY KEY)")
    schema = {
        "empty_tbl": {
            "columns": [{"name": "id", "type": "INTEGER", "nullable": False}],
            "primary_keys": ["id"],
            "foreign_keys": [],
            "sample_rows": [],
        }
    }
    analyzer = SchemaAnalyzer(schema, engine)
    assert analyzer.row_counts()["empty_tbl"] == 0
