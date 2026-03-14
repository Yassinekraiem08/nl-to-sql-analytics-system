"""Tests for ambiguity detection."""
from __future__ import annotations

import pytest

from app.core.ambiguity import detect_ambiguity, AmbiguityResult


# ---------------------------------------------------------------------------
# Non-ambiguous questions
# ---------------------------------------------------------------------------

def test_clear_question_not_ambiguous():
    r = detect_ambiguity("Count users registered in January 2024")
    assert r.is_ambiguous is False
    assert r.vague_terms == []
    assert r.suggestions == []


def test_analytics_question_not_ambiguous():
    r = detect_ambiguity("Total revenue by product category for Q1 2024")
    assert r.is_ambiguous is False


def test_count_query_not_ambiguous():
    r = detect_ambiguity("Count orders where status is completed")
    assert r.is_ambiguous is False


def test_empty_question_not_ambiguous():
    r = detect_ambiguity("")
    assert r.is_ambiguous is False


# ---------------------------------------------------------------------------
# Temporal vague terms
# ---------------------------------------------------------------------------

def test_recent_flagged():
    r = detect_ambiguity("Show me recent orders")
    assert r.is_ambiguous is True
    assert "recent" in r.vague_terms


def test_latest_flagged():
    r = detect_ambiguity("Get the latest products")
    assert r.is_ambiguous is True
    assert "latest" in r.vague_terms


def test_new_flagged():
    r = detect_ambiguity("Find new customers")
    assert r.is_ambiguous is True
    assert "new" in r.vague_terms


# ---------------------------------------------------------------------------
# Quantity vague terms
# ---------------------------------------------------------------------------

def test_some_flagged():
    r = detect_ambiguity("Show some orders")
    assert r.is_ambiguous is True
    assert "some" in r.vague_terms


def test_many_flagged():
    r = detect_ambiguity("Users who made many purchases")
    assert r.is_ambiguous is True
    assert "many" in r.vague_terms


def test_few_flagged():
    r = detect_ambiguity("Products with few reviews")
    assert r.is_ambiguous is True
    assert "few" in r.vague_terms


# ---------------------------------------------------------------------------
# Quality vague terms
# ---------------------------------------------------------------------------

def test_top_flagged():
    r = detect_ambiguity("Who are the top users?")
    assert r.is_ambiguous is True
    assert "top" in r.vague_terms


def test_best_flagged():
    r = detect_ambiguity("Best selling products")
    assert r.is_ambiguous is True
    assert "best" in r.vague_terms


def test_large_flagged():
    r = detect_ambiguity("Show large orders")
    assert r.is_ambiguous is True
    assert "large" in r.vague_terms


# ---------------------------------------------------------------------------
# Multiple vague terms
# ---------------------------------------------------------------------------

def test_multiple_vague_terms():
    r = detect_ambiguity("Show some recent large orders")
    assert r.is_ambiguous is True
    assert len(r.vague_terms) >= 2
    assert len(r.suggestions) >= 2


# ---------------------------------------------------------------------------
# Whole-word matching (no false positives)
# ---------------------------------------------------------------------------

def test_no_false_positive_latency_vs_latest():
    # "latency" contains "lat" but not the word "latest"
    r = detect_ambiguity("What is the average query latency?")
    assert r.is_ambiguous is False


def test_no_false_positive_current_vs_currently():
    # "currently" is not in the vague terms list
    r = detect_ambiguity("How many sessions are currently active?")
    # "current" is not in "currently" as a standalone word
    assert "current" not in r.vague_terms


# ---------------------------------------------------------------------------
# Case insensitivity
# ---------------------------------------------------------------------------

def test_case_insensitive_detection():
    r = detect_ambiguity("RECENT orders with HIGH value")
    assert r.is_ambiguous is True
    assert "recent" in r.vague_terms
    assert "high" in r.vague_terms


# ---------------------------------------------------------------------------
# warning_text helper
# ---------------------------------------------------------------------------

def test_warning_text_empty_when_not_ambiguous():
    r = detect_ambiguity("Count users by city")
    assert r.warning_text() == ""


def test_warning_text_non_empty_when_ambiguous():
    r = detect_ambiguity("Show recent large orders")
    text = r.warning_text()
    assert text != ""
    assert "Ambiguous" in text
