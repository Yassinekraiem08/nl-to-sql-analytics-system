from functools import lru_cache

from sqlalchemy import create_engine, Engine

from config import settings


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    """Return a cached SQLAlchemy engine.

    For PostgreSQL the pool is kept alive; for SQLite check_same_thread is
    disabled so it works across threads (FastAPI / tests).
    """
    url = settings.database_url
    kwargs: dict = {}
    if url.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False}
    return create_engine(url, **kwargs)
