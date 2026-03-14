import pytest

from app.core.sql_validator import SQLValidator, SQLValidationError

validator = SQLValidator()


SAFE_QUERIES = [
    "SELECT * FROM users",
    "SELECT id, name FROM users WHERE id = 1",
    "SELECT COUNT(*) FROM orders GROUP BY status",
    "SELECT u.name, COUNT(o.id) FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.name",
]

DANGEROUS_QUERIES = [
    "DROP TABLE users",
    "INSERT INTO users (name) VALUES ('hack')",
    "UPDATE users SET name='x' WHERE 1=1",
    "DELETE FROM users",
    "TRUNCATE TABLE users",
    "CREATE TABLE evil (id INT)",
    "ALTER TABLE users ADD COLUMN pw TEXT",
    "SELECT * FROM users; DROP TABLE users",
]


@pytest.mark.parametrize("sql", SAFE_QUERIES)
def test_safe_queries_pass(sql):
    validator.validate(sql)  # should not raise


@pytest.mark.parametrize("sql", DANGEROUS_QUERIES)
def test_dangerous_queries_blocked(sql):
    with pytest.raises(SQLValidationError):
        validator.validate(sql)


def test_empty_sql_raises():
    with pytest.raises(SQLValidationError):
        validator.validate("")


def test_whitespace_sql_raises():
    with pytest.raises(SQLValidationError):
        validator.validate("   ")
