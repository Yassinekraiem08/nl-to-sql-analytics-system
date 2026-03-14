import pytest
from sqlalchemy import create_engine, text


@pytest.fixture(scope="module")
def sqlite_engine():
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT
            )
        """))
        conn.execute(text("""
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                amount REAL,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """))
        conn.execute(text("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')"))
        conn.execute(text("INSERT INTO orders VALUES (1, 1, 99.99)"))
        conn.commit()
    return engine


def test_schema_loader_returns_tables(sqlite_engine):
    from app.core.schema_loader import SchemaLoader
    loader = SchemaLoader(sqlite_engine)
    schema = loader.load()
    assert "users" in schema
    assert "orders" in schema


def test_schema_loader_columns(sqlite_engine):
    from app.core.schema_loader import SchemaLoader
    loader = SchemaLoader(sqlite_engine)
    schema = loader.load()
    col_names = [c["name"] for c in schema["users"]["columns"]]
    assert "id" in col_names
    assert "name" in col_names
    assert "email" in col_names


def test_schema_loader_sample_rows(sqlite_engine):
    from app.core.schema_loader import SchemaLoader
    loader = SchemaLoader(sqlite_engine)
    schema = loader.load()
    assert len(schema["users"]["sample_rows"]) >= 1


def test_schema_loader_table_method(sqlite_engine):
    from app.core.schema_loader import SchemaLoader
    loader = SchemaLoader(sqlite_engine)
    table = loader.table("users")
    assert "columns" in table


def test_schema_loader_unknown_table_raises(sqlite_engine):
    from app.core.schema_loader import SchemaLoader
    loader = SchemaLoader(sqlite_engine)
    with pytest.raises(KeyError):
        loader.table("nonexistent_table")


def test_schema_loader_cache(sqlite_engine):
    from app.core.schema_loader import SchemaLoader
    loader = SchemaLoader(sqlite_engine)
    schema1 = loader.load()
    schema2 = loader.load()
    assert schema1 is schema2  # same object = cached
