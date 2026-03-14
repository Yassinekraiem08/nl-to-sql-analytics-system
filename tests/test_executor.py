import pytest
import pandas as pd
from sqlalchemy import create_engine, text

from app.core.sql_executor import SQLExecutor, SQLExecutionError


@pytest.fixture(scope="module")
def engine_with_data():
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)"))
        for i in range(1, 6):
            conn.execute(text(f"INSERT INTO products VALUES ({i}, 'Product {i}', {i * 10.0})"))
        conn.commit()
    return engine


def test_executor_returns_dataframe(engine_with_data):
    executor = SQLExecutor(engine_with_data)
    df = executor.execute("SELECT * FROM products")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 5


def test_executor_columns(engine_with_data):
    executor = SQLExecutor(engine_with_data)
    df = executor.execute("SELECT id, name FROM products")
    assert list(df.columns) == ["id", "name"]


def test_executor_row_limit(engine_with_data):
    executor = SQLExecutor(engine_with_data, max_rows=3)
    df = executor.execute("SELECT * FROM products")
    assert len(df) <= 3


def test_executor_bad_sql_raises(engine_with_data):
    executor = SQLExecutor(engine_with_data)
    with pytest.raises(SQLExecutionError):
        executor.execute("SELECT * FROM nonexistent_table_xyz")


def test_executor_empty_result(engine_with_data):
    executor = SQLExecutor(engine_with_data)
    df = executor.execute("SELECT * FROM products WHERE id = 9999")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0
