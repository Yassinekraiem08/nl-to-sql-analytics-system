"""Tests for ExampleStore, TFIDFEmbedder, and PromptBuilder few-shot injection."""
from __future__ import annotations

import math
import tempfile
from pathlib import Path

import pytest

from app.core.example_store import ExampleStore, TFIDFEmbedder, FewShotExample
from app.core.prompt_builder import PromptBuilder
from app.core.schema_analyzer import RelationshipGraph


# ---------------------------------------------------------------------------
# TFIDFEmbedder
# ---------------------------------------------------------------------------

def test_tfidf_embed_returns_fixed_dim():
    emb = TFIDFEmbedder()
    vec = emb.embed("how many users are there")
    assert len(vec) == TFIDFEmbedder.DIM


def test_tfidf_embed_is_unit_vector():
    emb = TFIDFEmbedder()
    vec = emb.embed("top 10 customers by revenue")
    norm = math.sqrt(sum(x * x for x in vec))
    assert abs(norm - 1.0) < 1e-6


def test_tfidf_embed_is_deterministic():
    emb = TFIDFEmbedder()
    text = "revenue by product category"
    assert emb.embed(text) == emb.embed(text)


def test_tfidf_similar_texts_have_higher_similarity():
    import numpy as np
    emb = TFIDFEmbedder()
    q = "top customers by total spend"
    similar = "best customers ranked by spending"
    unrelated = "what is the weather in paris"

    vq = np.array(emb.embed(q))
    vs = np.array(emb.embed(similar))
    vu = np.array(emb.embed(unrelated))

    assert vq @ vs > vq @ vu


def test_tfidf_empty_string_returns_zero_vector():
    emb = TFIDFEmbedder()
    vec = emb.embed("")
    assert all(x == 0.0 for x in vec)


# ---------------------------------------------------------------------------
# ExampleStore — basic operations
# ---------------------------------------------------------------------------

@pytest.fixture()
def store(tmp_path: Path) -> ExampleStore:
    return ExampleStore(tmp_path / "examples")


def test_store_starts_empty(store: ExampleStore):
    assert store.size == 0


def test_store_add_increases_size(store: ExampleStore):
    store.add("How many users?", "SELECT COUNT(*) FROM users", db_id="demo")
    assert store.size == 1


def test_store_retrieve_empty_returns_nothing(store: ExampleStore):
    assert store.retrieve("anything") == []


def test_store_retrieve_returns_examples(store: ExampleStore):
    store.add("How many users?", "SELECT COUNT(*) FROM users", db_id="demo")
    store.add("List all categories", "SELECT DISTINCT category FROM products", db_id="demo")
    results = store.retrieve("count users", k=2)
    assert len(results) == 2
    assert all(isinstance(r, FewShotExample) for r in results)


def test_store_retrieve_k_caps_at_store_size(store: ExampleStore):
    store.add("Q1", "SELECT 1", db_id="demo")
    results = store.retrieve("anything", k=10)
    assert len(results) == 1


def test_store_retrieve_ranks_by_similarity(store: ExampleStore):
    store.add("How many users are there?", "SELECT COUNT(*) FROM users", db_id="demo")
    store.add("List all product categories", "SELECT DISTINCT category FROM products", db_id="demo")
    store.add("Total revenue by category", "SELECT category, SUM(...) FROM ...", db_id="demo")

    results = store.retrieve("count the number of users", k=1)
    assert "user" in results[0].question.lower()


def test_store_retrieve_filters_by_db_id(store: ExampleStore):
    store.add("Q1", "SELECT 1", db_id="db_a")
    store.add("Q2", "SELECT 2", db_id="db_b")

    results_a = store.retrieve("Q1", k=5, db_id="db_a")
    assert all(r.db_id == "db_a" for r in results_a)

    results_b = store.retrieve("Q2", k=5, db_id="db_b")
    assert all(r.db_id == "db_b" for r in results_b)


def test_store_retrieve_db_id_filter_no_match_returns_empty(store: ExampleStore):
    store.add("Q1", "SELECT 1", db_id="demo")
    assert store.retrieve("Q1", db_id="other_db") == []


def test_store_clear(store: ExampleStore):
    store.add("Q", "SELECT 1", db_id="demo")
    store.clear()
    assert store.size == 0
    assert store.retrieve("Q") == []


# ---------------------------------------------------------------------------
# ExampleStore — persistence
# ---------------------------------------------------------------------------

def test_store_persists_across_instances(tmp_path: Path):
    path = tmp_path / "examples"
    s1 = ExampleStore(path)
    s1.add("How many orders?", "SELECT COUNT(*) FROM orders", db_id="demo")

    s2 = ExampleStore(path)
    assert s2.size == 1
    assert s2._examples[0].question == "How many orders?"


def test_store_vectors_reloaded_correctly(tmp_path: Path):
    import numpy as np
    path = tmp_path / "examples"
    s1 = ExampleStore(path)
    s1.add("revenue by category", "SELECT category, SUM(price) FROM products GROUP BY category")

    s2 = ExampleStore(path)
    results = s2.retrieve("total revenue per category", k=1)
    assert len(results) == 1


# ---------------------------------------------------------------------------
# PromptBuilder few-shot injection
# ---------------------------------------------------------------------------

SIMPLE_SCHEMA = {
    "users": {
        "columns": [{"name": "id", "type": "INTEGER"}, {"name": "name", "type": "TEXT"}],
        "primary_keys": ["id"],
        "foreign_keys": [],
        "sample_rows": [],
    }
}


def test_prompt_builder_without_store_has_no_examples():
    builder = PromptBuilder(SIMPLE_SCHEMA)
    messages = builder.build("How many users are there?")
    assert len(messages) == 1
    assert "Reference examples" not in messages[0]["content"]


def test_prompt_builder_with_empty_store_has_no_examples(store: ExampleStore):
    builder = PromptBuilder(SIMPLE_SCHEMA, example_store=store)
    messages = builder.build("How many users are there?")
    assert "Reference examples" not in messages[0]["content"]


def test_prompt_builder_injects_examples_when_store_populated(store: ExampleStore):
    store.add("Count all users", "SELECT COUNT(*) FROM users", db_id="demo")
    builder = PromptBuilder(SIMPLE_SCHEMA, example_store=store)
    messages = builder.build("How many users are there?")
    content = messages[0]["content"]
    assert "Reference examples" in content
    assert "SELECT COUNT(*) FROM users" in content


def test_prompt_builder_injects_up_to_k_examples(store: ExampleStore):
    for i in range(5):
        store.add(f"Question {i}", f"SELECT {i}", db_id="demo")
    builder = PromptBuilder(SIMPLE_SCHEMA, example_store=store, few_shot_k=2)
    messages = builder.build("some question")
    content = messages[0]["content"]
    # Exactly 2 SQL code blocks should appear in the examples section
    import re
    sql_blocks = re.findall(r"```sql", content)
    assert len(sql_blocks) == 2


def test_prompt_builder_examples_appear_before_question(store: ExampleStore):
    store.add("Count users", "SELECT COUNT(*) FROM users", db_id="demo")
    builder = PromptBuilder(SIMPLE_SCHEMA, example_store=store)
    messages = builder.build("How many users?")
    content = messages[0]["content"]
    examples_pos = content.index("Reference examples")
    question_pos = content.index("Question: How many users?")
    assert examples_pos < question_pos
