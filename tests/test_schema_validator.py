"""Tests for SchemaValidator — table/column existence checks and fuzzy suggestions."""
import pytest

from app.core.schema_validator import SchemaValidator, SchemaIssue, _similarity, _fuzzy_best


# ------------------------------------------------------------------
# Shared schema fixture
# ------------------------------------------------------------------

SCHEMA = {
    "users": {
        "columns": [
            {"name": "id", "type": "INTEGER"},
            {"name": "name", "type": "TEXT"},
            {"name": "email", "type": "TEXT"},
            {"name": "created_at", "type": "TIMESTAMP"},
        ],
        "primary_keys": ["id"],
        "foreign_keys": [],
        "sample_rows": [],
    },
    "orders": {
        "columns": [
            {"name": "id", "type": "INTEGER"},
            {"name": "user_id", "type": "INTEGER"},
            {"name": "total", "type": "REAL"},
            {"name": "status", "type": "TEXT"},
        ],
        "primary_keys": ["id"],
        "foreign_keys": [{"columns": ["user_id"], "referred_table": "users", "referred_columns": ["id"]}],
        "sample_rows": [],
    },
}

v = SchemaValidator()


# ------------------------------------------------------------------
# Happy paths — no issues expected
# ------------------------------------------------------------------

def test_simple_select_no_issues():
    issues = v.validate("SELECT id, name FROM users", SCHEMA)
    assert issues == []


def test_join_query_no_issues():
    sql = "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id"
    issues = v.validate(sql, SCHEMA)
    assert issues == []


def test_aggregation_no_issues():
    sql = "SELECT status, COUNT(*) FROM orders GROUP BY status"
    issues = v.validate(sql, SCHEMA)
    assert issues == []


def test_subquery_no_issues():
    sql = "SELECT * FROM (SELECT id, name FROM users) sub"
    issues = v.validate(sql, SCHEMA)
    assert issues == []


def test_string_literal_not_flagged_as_table():
    # 'FROM' inside a string value should not be treated as a table reference
    sql = "SELECT name FROM users WHERE name = 'John FROM London'"
    issues = v.validate(sql, SCHEMA)
    assert issues == []


# ------------------------------------------------------------------
# Table existence errors
# ------------------------------------------------------------------

def test_nonexistent_table_flagged():
    issues = v.validate("SELECT * FROM usr", SCHEMA)
    errors = [i for i in issues if i.severity == "error"]
    assert any("usr" in i.message for i in errors)


def test_nonexistent_table_suggests_correction():
    issues = v.validate("SELECT * FROM usr", SCHEMA)
    assert any(i.suggestion and "users" in i.suggestion for i in issues)


def test_nonexistent_table_in_join_flagged():
    sql = "SELECT * FROM users JOIN ordrs ON users.id = ordrs.user_id"
    issues = v.validate(sql, SCHEMA)
    assert any("ordrs" in i.message for i in issues)


# ------------------------------------------------------------------
# Column existence errors
# ------------------------------------------------------------------

def test_hallucinated_column_flagged():
    sql = "SELECT u.usr_name FROM users u"
    issues = v.validate(sql, SCHEMA)
    assert any("usr_name" in i.message for i in issues)


def test_hallucinated_column_suggests_correction():
    sql = "SELECT u.usr_name FROM users u"
    issues = v.validate(sql, SCHEMA)
    assert any(i.suggestion and "name" in i.suggestion for i in issues)


def test_valid_qualified_column_no_issue():
    sql = "SELECT u.name, u.email FROM users u"
    issues = v.validate(sql, SCHEMA)
    assert issues == []


def test_column_on_wrong_table_flagged():
    # 'total' exists on orders, not on users
    sql = "SELECT u.total FROM users u"
    issues = v.validate(sql, SCHEMA)
    assert any("total" in i.message for i in issues)


def test_column_case_insensitive_no_false_positive():
    # Most DBs are case-insensitive on column names
    sql = "SELECT u.Name FROM users u"
    issues = v.validate(sql, SCHEMA)
    assert issues == []


# ------------------------------------------------------------------
# Fuzzy matching internals
# ------------------------------------------------------------------

def test_similarity_identical():
    assert _similarity("users", "users") == 1.0


def test_similarity_close():
    assert _similarity("usr", "user") > 0.0


def test_similarity_unrelated_is_low():
    assert _similarity("xyz", "abcdef") < 0.3


def test_fuzzy_best_finds_close_match():
    result = _fuzzy_best("ordrs", {"users", "orders", "products"})
    assert result == "orders"


def test_fuzzy_best_returns_none_for_garbage():
    result = _fuzzy_best("zzzzz", {"users", "orders"})
    assert result is None


# ------------------------------------------------------------------
# SchemaIssue string representation
# ------------------------------------------------------------------

def test_issue_str_with_suggestion():
    issue = SchemaIssue(severity="error", message="Table 'usr' not found.", suggestion="Did you mean 'users'?")
    s = str(issue)
    assert "[ERROR]" in s
    assert "Did you mean" in s


def test_issue_str_without_suggestion():
    issue = SchemaIssue(severity="warning", message="Something looks off.")
    assert "Something looks off" in str(issue)
